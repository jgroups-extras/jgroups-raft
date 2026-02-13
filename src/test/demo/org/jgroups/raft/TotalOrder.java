package org.jgroups.raft;

import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.raft.configuration.RuntimeProperties;
import org.jgroups.util.Util;

import java.awt.BorderLayout;
import java.awt.Button;
import java.awt.Canvas;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Frame;
import java.awt.Graphics;
import java.awt.Image;
import java.awt.Menu;
import java.awt.MenuBar;
import java.awt.MenuItem;
import java.awt.Panel;
import java.awt.Point;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import javax.management.MBeanServer;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

public class TotalOrder extends Frame {
    private static final Font def_font=new Font("Helvetica", Font.BOLD, 12);
    private static final Font def_font2=new Font("Helvetica", Font.PLAIN, 12);
    final MenuBar menubar=createMenuBar();
    final Button start=new Button("Start");
    final Button stop=new Button("Stop");
    final Button clear=new Button("Clear");
    final Button quit=new Button("Quit");
    final Panel button_panel=new Panel();

    TotalOrder.SenderThread sender=null;
    long timeout=0;
    int field_size=0;
    int num_fields=0;
    static final int x_offset=30;
    static final int y_offset=40;
    private int num=0;

    private JGroupsRaft<CanvasStateMachine> raft;

    private int num_additions=0, num_subtractions=0, num_divisions=0, num_multiplications=0;


    static void error(String s) {
        System.err.println(s);
    }

    static class EventHandler extends WindowAdapter {
        final Frame gui;

        public EventHandler(Frame g) {
            gui=g;
        }

        public void windowClosing(WindowEvent e) {
            gui.dispose();
            System.exit(0);
        }
    }


    final class SenderThread extends Thread {
        TotOrderRequest req;
        boolean running=true;

        public void stopSender() {
            running=false;
            interrupt();
            System.out.println("-- num_additions: " + num_additions +
                    "\n-- num_subtractions: " + num_subtractions +
                    "\n-- num_divisions: " + num_divisions +
                    "\n-- num_multiplications: " + num_multiplications);
            num_additions=num_subtractions=num_multiplications=num_divisions=0;
        }

        public void run() {
            this.setName("SenderThread");
            int cnt=0;
            while(running) {
                try {
                    req=createRandomRequest();
                    switch (req.type) {
                        case TotOrderRequest.ADDITION:
                            raft.write(c -> {
                                c.addValueTo(req.x, req.y, req.val);
                            });
                            num_additions++;
                            break;
                        case TotOrderRequest.SUBTRACTION:
                            raft.write( c -> {
                                c.subtractValueFrom(req.x, req.y, req.val);
                            });
                            num_subtractions++;
                            break;
                        case TotOrderRequest.MULTIPLICATION:
                            raft.write(c -> {
                                c.multiplyValueWith(req.x, req.y, req.val);
                            });
                            num_multiplications++;
                            break;
                        case TotOrderRequest.DIVISION:
                            raft.write(c -> {
                                c.divideValueBy(req.x, req.y, req.val);
                            });
                            num_divisions++;
                            break;
                    }
                    //System.out.print("-- num requests sent: " + cnt + "\r");
                    if(timeout > 0)
                        Util.sleep(timeout);
                    cnt++;
                    if(num > 0 && cnt > num) {
                        running=false;
                        cnt=0;
                    }
                } catch(Exception e) {
                    error(e.toString());
                    return;
                }
            }
        }
    }


    public TotalOrder(String title, long timeout, int num_fields, int field_size, String props, int num) {
        Dimension s;

        this.timeout=timeout;
        this.num_fields=num_fields;
        this.field_size=field_size;
        this.num=num;
        setFont(def_font);
        start.addActionListener(e -> startSender());

        stop.addActionListener(e -> stopSender());

        clear.addActionListener(e -> raft.write(CanvasStateMachine::clear));

        quit.addActionListener(e -> {
            raft.stop();
            System.exit(0);
        });

        setTitle(title);
        addWindowListener(new TotalOrder.EventHandler(this));
        setBackground(Color.white);
        setMenuBar(menubar);

        setLayout(new BorderLayout());

        CanvasStateMachine.Impl canvas = new CanvasStateMachine.Impl(num_fields, field_size, x_offset, y_offset);
        add("Center", canvas);
        button_panel.setLayout(new FlowLayout());
        button_panel.setFont(def_font2);
        button_panel.add(start);
        button_panel.add(stop);
        button_panel.add(clear);
        button_panel.add(quit);
        add("South", button_panel);

        s=canvas.getSize();
        s.height+=100;
        setSize(s);

        try {
            JChannel channel=new JChannel(props);
            channel.setReceiver(new Receiver() {
                public void viewAccepted(View view) {
                    System.out.println("view = " + view);
                }
            });
            raft = JGroupsRaft.builder(canvas, CanvasStateMachine.class)
                    .withJChannel(channel)
                    .withClusterName("total-order")
                    .registerSerializationContextInitializer(new TotalOrderSerializationInitializerImpl())
                    .withRuntimeProperties(RuntimeProperties.from(Map.of(JGroupsRaftMetrics.METRICS_ENABLED.name(), "true")))
                    .registerMarshaller(new JGroupsRaftCustomMarshaller<int[][]>() {
                        @Override
                        public Class<? extends int[][]> javaClass() {
                            return int[][].class;
                        }

                        @Override
                        public byte[] write(int[][] obj) {
                            int size = Integer.BYTES * obj.length;
                            for (int[] ints : obj) {
                                size += Integer.BYTES + Integer.BYTES * ints.length;
                            }
                            byte[] datum = new byte[size];
                            ByteBuffer buffer = ByteBuffer.wrap(datum);
                            buffer.putInt(obj.length);
                            for (int[] ints : obj) {
                                buffer.putInt(ints.length);
                                for (int i : ints) {
                                    buffer.putInt(i);
                                }
                            }
                            return datum;
                        }

                        @Override
                        public int[][] read(byte[] datum) {
                            ByteBuffer buffer = ByteBuffer.wrap(datum);
                            int outer = buffer.getInt();
                            int[][] output = new int[outer][];
                            for (int i = 0; i < outer; i++) {
                                int inner = buffer.getInt();
                                output[i] = new int[inner];
                                for (int j = 0; j < inner; j++) {
                                    output[i][j] = buffer.getInt();
                                }
                            }
                            return output;
                        }
                    })
                    .build();
            raft.start();

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            JmxConfigurator.registerChannel(channel, server, "jgroups", channel.getClusterName(), true);
        } catch(Exception e) {
            e.printStackTrace(System.err);
            System.exit(-1);
        }
    }


    void startSender() {
        if(sender == null || !sender.isAlive()) {
            sender=new TotalOrder.SenderThread();
            sender.start();
        }
    }

    void stopSender() {
        if(sender != null) {
            sender.stopSender();
            sender=null;
        }
    }

    private static MenuBar createMenuBar() {
        MenuBar ret=new MenuBar();
        Menu file=new Menu("File");
        MenuItem quitm=new MenuItem("Quit");

        ret.setFont(def_font2);
        ret.add(file);

        file.addSeparator();
        file.add(quitm);

        quitm.addActionListener(e -> System.exit(1));
        return ret;
    }

    private TotOrderRequest createRandomRequest() {
        TotOrderRequest ret=null;
        byte op_type=(byte)(((Math.random() * 10) % 4) + 1);  // 1 - 4
        int x=(int)((Math.random() * num_fields * 2) % num_fields);
        int y=(int)((Math.random() * num_fields * 2) % num_fields);
        int val=(int)((Math.random() * num_fields * 200) % 10);

        ret= new TotOrderRequest(op_type, x, y, val);
        return ret;
    }


    public static void main(String[] args) {
        TotalOrder g;
        String arg;
        long timeout=200;
        int num_fields=5;
        int field_size=80;
        String props=null;
        int num=0;

        props="raft.xml";


        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-timeout".equals(arg)) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if("-num_fields".equals(arg)) {
                num_fields=Integer.parseInt(args[++i]);
                continue;
            }
            if("-field_size".equals(arg)) {
                field_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-help".equals(arg)) {
                help();
                return;
            }
            if("-props".equals(arg)) {
                props=args[++i];
                continue;
            }
            if("-num".equals(arg)) {
                num=Integer.parseInt(args[++i]);
            }
            help();
            return;
        }


        try {
            g=new TotalOrder("Total Order Demo on " + InetAddress.getLocalHost().getHostName(),
                    timeout, num_fields, field_size, props, num);
            g.setVisible(true);
        }
        catch(Exception e) {
            e.printStackTrace(System.err);
        }
    }

    protected static void help() {
        System.out.println("""

                TotalOrder [-timeout <value>] [-num_fields <value>] \
                [-field_size <value>] [-props <properties (can be URL)>] [-num <num requests>]
                """);
    }

    @JGroupsRaftStateMachine
    interface CanvasStateMachine {

        @StateMachineWrite(id = 1)
        void addValueTo(int x, int y, int value);

        @StateMachineWrite(id = 2)
        void subtractValueFrom(int x, int y, int value);

        @StateMachineWrite(id = 3)
        void multiplyValueWith(int x, int y, int value);

        @StateMachineWrite(id = 4)
        void divideValueBy(int x, int y, int value);

        @StateMachineWrite(id = 5)
        void clear();

        @StateMachineRead(id = 6)
        int[][] getState();

        class Impl extends Canvas implements TotalOrder.CanvasStateMachine {
            static final Color checksum_col=Color.blue;

            int field_size=100;
            int num_fields=4;
            int x_offset=30;
            int y_offset=30;

            final Font def_font=new Font("Helvetica", Font.BOLD, 14);

            @StateMachineField(order = 0)
            int[][] array;      // state

            Dimension off_dimension=null;
            Image off_image=null;
            Graphics off_graphics=null;
            final Font def_font2=new Font("Helvetica", Font.PLAIN, 12);
            int checksum=0;

            public Impl(int num_fields, int field_size, int x_offset, int y_offset) {
                this.num_fields=num_fields;
                this.field_size=field_size;
                this.x_offset=x_offset;
                this.y_offset=y_offset;

                array=new int[num_fields][num_fields];
                setBackground(Color.white);
                setSize(2 * x_offset + num_fields * field_size + 30, y_offset + num_fields * field_size + 50);

                for(int i=0; i < num_fields; i++)
                    for(int j=0; j < num_fields; j++)
                        array[i][j]=0;
            }

            public void addValueTo(int x, int y, int value) {
                array[x][y]+=value;
                update();
            }

            public void subtractValueFrom(int x, int y, int value) {
                array[x][y]-=value;
                update();
            }

            public void multiplyValueWith(int x, int y, int value) {
                array[x][y]*=value;
                update();
            }

            public void divideValueBy(int x, int y, int value) {
                if(value == 0)
                    return;

                array[x][y]/=value;
                update();
            }

            public void clear() {
                for(int i=0; i < num_fields; i++)
                    for(int j=0; j < num_fields; j++)
                        array[i][j]=0;
                update();
            }

            public int[][] getState() {
                return array;
            }

            private void update() {
                checksum=checksum();
                repaint();
            }

            private int checksum() {
                int retval=0;

                for(int i=0; i < num_fields; i++)
                    for(int j=0; j < num_fields; j++)
                        retval+=array[i][j];
                return retval;
            }

            @Override
            public void update(Graphics g) {
                Dimension d=getSize();

                if(off_graphics == null ||
                        d.width != off_dimension.width ||
                        d.height != off_dimension.height) {
                    off_dimension=d;
                    off_image=createImage(d.width, d.height);
                    off_graphics=off_image.getGraphics();
                }

                //Erase the previous image.
                off_graphics.setColor(getBackground());
                off_graphics.fillRect(0, 0, d.width, d.height);
                off_graphics.setColor(Color.black);
                off_graphics.setFont(def_font);
                drawEmptyBoard(off_graphics);
                drawNumbers(off_graphics);
                g.drawImage(off_image, 0, 0, this);
            }

            @Override
            public void paint(Graphics g) {
                update(g);
            }

            /**
             * Draws the empty board, no pieces on it yet, just grid lines
             */
            private void drawEmptyBoard(Graphics g) {
                int x=x_offset, y=y_offset;
                Color old_col=g.getColor();

                g.setFont(def_font2);
                old_col=g.getColor();
                g.setColor(checksum_col);
                g.drawString(("Checksum: " + checksum), x_offset + field_size, y_offset - 20);
                g.setFont(def_font);
                g.setColor(old_col);

                for(int i=0; i < num_fields; i++) {
                    for(int j=0; j < num_fields; j++) {  // draws 1 row
                        g.drawRect(x, y, field_size, field_size);
                        x+=field_size;
                    }
                    g.drawString((String.valueOf((num_fields - i - 1))), x + 20, y + field_size / 2);
                    y+=field_size;
                    x=x_offset;
                }

                for(int i=0; i < num_fields; i++) {
                    g.drawString((String.valueOf(i)), x_offset + i * field_size + field_size / 2, y + 30);
                }
            }

            private void drawNumbers(Graphics g) {
                Point p;
                String num;
                FontMetrics fm=g.getFontMetrics();
                int len=0;

                for(int i=0; i < num_fields; i++)
                    for(int j=0; j < num_fields; j++) {
                        num=String.valueOf(array[i][j]);
                        len=fm.stringWidth(num);
                        p=index2Coord(i, j);
                        g.drawString(num, p.x - (len / 2), p.y);
                    }
            }

            private Point index2Coord(int i, int j) {
                int x=x_offset + i * field_size + field_size / 2;
                int y=y_offset + num_fields * field_size - j * field_size - field_size / 2;
                return new Point(x, y);
            }
        }
    }

    @Proto
    record TotOrderRequest(@ProtoField(number = 1, defaultValue = "0") byte type,
                           @ProtoField(number = 2, defaultValue = "0") int x,
                           @ProtoField(number = 3, defaultValue = "0") int y,
                           @ProtoField(number = 4, defaultValue = "0") int val) {

        public static final byte ADDITION = 1;
        public static final byte SUBTRACTION = 2;
        public static final byte MULTIPLICATION = 3;
        public static final byte DIVISION = 4;

        public String printType() {
            return switch (type) {
                case ADDITION -> "ADDITION";
                case SUBTRACTION -> "SUBTRACTION";
                case MULTIPLICATION -> "MULTIPLICATION";
                case DIVISION -> "DIVISION";
                default -> "<unknown>";
            };
        }

        public String toString() {
            return "[" + x + ',' + y + ": " + printType() + '(' + val + ")]";
        }
    }


    @ProtoSchema(
            includeClasses = TotOrderRequest.class,
            schemaFileName = "total-order.proto",
            schemaFilePath = "proto/generated",
            schemaPackageName = "org.jgroups.demo",
            service = false,
            syntax = ProtoSyntax.PROTO3
    )
    public interface TotalOrderSerializationInitializer extends SerializationContextInitializer { }
}

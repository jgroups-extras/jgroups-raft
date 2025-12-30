package org.jgroups.raft.tests.listeners;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.IAnnotationTransformer;
import org.testng.IConfigurationListener;
import org.testng.ISuiteListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.annotations.Test;

public class TestNGTestListener implements ITestListener, IConfigurationListener, ISuiteListener, IAnnotationTransformer {

    private static final String KEY_TEST_NAME = "testName";
    private static final Logger LOG = LogManager.getLogger(TestNGTestListener.class);

    private final TestSuiteProgress progressLogger;

    public TestNGTestListener() {
        progressLogger = new TestSuiteProgress();
    }

    @Override
    public void onTestStart(ITestResult result) {
        TestResourceTracker.setTestName(result.getTestClass().getName());
        progressLogger.started(testName(result));
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        progressLogger.succeeded(testName(result));
        cleanup();
    }

    @Override
    public void onTestFailure(ITestResult result) {
        progressLogger.failed(testName(result), result.getThrowable());
        cleanup();
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        progressLogger.ignored(testName(result));
        cleanup();
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        progressLogger.failed(testName(result), result.getThrowable());
        cleanup();
    }

    @Override
    public void onStart(ITestContext context) {
        Thread.currentThread().setName("TestNG-" + context.getName());
    }

    @Override
    public void beforeConfiguration(ITestResult tr) {
        TestResourceTracker.setTestName(tr.getTestClass().getName());
        progressLogger.configurationStarted(testName(tr));
    }

    @Override
    public void onConfigurationSuccess(ITestResult tr) {
        progressLogger.configurationFinished(testName(tr));
    }

    @Override
    public void onConfigurationFailure(ITestResult tr) {
        progressLogger.configurationFailed(testName(tr), tr.getThrowable());
        cleanup();
    }

    @Override
    public void onConfigurationSkip(ITestResult tr) {
        if (tr.getThrowable() != null)
            progressLogger.ignored(testName(tr));
    }

    private String testName(ITestResult res) {
        StringBuilder result = new StringBuilder();
        // We prefer using the instance name, in case it's customized,
        // but when running JUnit tests, TestNG sets the instance name to `methodName(className)`
        if (res.getInstanceName().contains(res.getMethod().getMethodName())) {
            result.append(res.getTestClass().getName());
        } else {
            result.append(res.getInstanceName());
        }
        result.append(".").append(res.getMethod().getMethodName());
        if (res.getMethod().getConstructorOrMethod().getMethod().isAnnotationPresent(Test.class)) {
            String dataProviderName = res.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class)
                    .dataProvider();
            // Add parameters for methods that use a data provider only
            if (res.getParameters().length != 0 && !dataProviderName.isEmpty()) {
                result.append("(").append(Arrays.deepToString(res.getParameters())).append(")");
            }
        }
        return result.toString();
    }

    private void cleanup() {
        // Clear the context so this thread can be reused cleanly
        TestResourceTracker.clear();
    }
}

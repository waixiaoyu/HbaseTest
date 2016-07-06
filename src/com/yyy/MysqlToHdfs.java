package com.yyy;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

public class MysqlToHdfs {
	private static SqoopClient client;
	static {
		// ��ʼ��
		String url = "http://node1:12000/sqoop/";
		client = new SqoopClient(url);
	}

	public static void main(String[] args) {
		//sqoopTransfer();
		startUpJob();
	}

	public static void startUpJob() {
		// ��������
		long jobId = 3;
		MSubmission submission = client.startJob(jobId);
		System.out.println("JOB�ύ״̬Ϊ : " + submission.getStatus());
		while (submission.getStatus().isRunning() && submission.getProgress() != -1) {
			System.out.println("���� : " + String.format("%.2f %%", submission.getProgress() * 100));
			// ���뱨��һ�ν���
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("JOBִ�н���... ...");
		System.out.println("Hadoop����IDΪ :" + submission.getExternalId());
		Counters counters = submission.getCounters();
		if (counters != null) {
			System.out.println("������:");
			for (CounterGroup group : counters) {
				System.out.print("\t");
				System.out.println(group.getName());
				for (Counter counter : group) {
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if (submission.getExceptionInfo() != null) {
			System.out.println("JOBִ���쳣���쳣��ϢΪ : " + submission.getExceptionInfo());
		}
		System.out.println("MySQLͨ��sqoop�������ݵ�HDFSͳ��ִ�����");
	}

	public static void sqoopTransfer() {

		// ����һ��Դ���� JDBC
		long fromConnectorId = 1;
		MLink fromLink = client.createLink(fromConnectorId);
		fromLink.setName("JDBC connector" + System.currentTimeMillis());
		fromLink.setCreationUser("admln");
		MLinkConfig fromLinkConfig = fromLink.getConnectorLinkConfig();
		fromLinkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://192.168.3.27:3306/test");
		fromLinkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		fromLinkConfig.getStringInput("linkConfig.username").setValue("root");
		fromLinkConfig.getStringInput("linkConfig.password").setValue("123123");
		Status fromStatus = client.saveLink(fromLink);
		if (fromStatus.canProceed()) {
			System.out.println("����JDBC Link�ɹ���IDΪ: " + fromLink.getPersistenceId());
		} else {
			System.out.println("����JDBC Linkʧ��");
		}
		// ����һ��Ŀ�ĵ�����HDFS
		long toConnectorId = 2;
		MLink toLink = client.createLink(toConnectorId);
		toLink.setName("HDFS connector" + System.currentTimeMillis());
		toLink.setCreationUser("admln");
		MLinkConfig toLinkConfig = toLink.getConnectorLinkConfig();
		toLinkConfig.getStringInput("linkConfig.uri").setValue("hdfs://node1:8020/");
		Status toStatus = client.saveLink(toLink);
		if (toStatus.canProceed()) {
			System.out.println("����HDFS Link�ɹ���IDΪ: " + toLink.getPersistenceId());
		} else {
			System.out.println("����HDFS Linkʧ��");
		}

		// ����һ������
		long fromLinkId = fromLink.getPersistenceId();
		long toLinkId = toLink.getPersistenceId();
		MJob job = client.createJob(fromLinkId, toLinkId);
		job.setName("MySQL to HDFS job" + System.currentTimeMillis());
		job.setCreationUser("admln");
		// ����Դ��������������Ϣ
		MFromConfig fromJobConfig = job.getFromJobConfig();
		fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("test");
		fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("data2");
		fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");
		MToConfig toJobConfig = job.getToJobConfig();
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/usr/tmp");

		Status status = client.saveJob(job);
		if (status.canProceed()) {
			System.out.println("JOB�����ɹ���IDΪ: " + job.getPersistenceId());
		} else {
			System.out.println("JOB����ʧ�ܡ�");
		}

	}
}
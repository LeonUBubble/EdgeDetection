package edu.zju.gis.mr;


public class GHQStatistics2 {
	
	public static void main(String[] args) {
		try {
			//Ŀ����Ϊ���ڱ��ؽ�����ĳ�ʼ��
			GHQStatistics2_1 ghq1 = new GHQStatistics2_1();
			GHQStatistics2_2 ghq2 = new GHQStatistics2_2();
			System.out.println("ִ�е�һ������!");
			ghq1.executor();
			System.out.println("ִ�еڶ�������!");
			ghq2.executor();
			System.out.println("����ִ����ϣ�");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

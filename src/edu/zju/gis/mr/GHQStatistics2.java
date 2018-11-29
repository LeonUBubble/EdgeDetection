package edu.zju.gis.mr;


public class GHQStatistics2 {
	
	public static void main(String[] args) {
		try {
			//目的是为了在本地进行类的初始化
			GHQStatistics2_1 ghq1 = new GHQStatistics2_1();
			GHQStatistics2_2 ghq2 = new GHQStatistics2_2();
			System.out.println("执行第一个任务!");
			ghq1.executor();
			System.out.println("执行第二个任务!");
			ghq2.executor();
			System.out.println("任务都执行完毕！");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

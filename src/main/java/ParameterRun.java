/**
 * @Description:
 * @Author: l0430
 * @Date: 2020/1/2 上午11:13
 */

import org.apache.flink.api.java.utils.ParameterTool;

public class ParameterRun {
    //配置文件路径
    private String configPath;

    //并行度
    private int parallelism = 1;

    //是否使能checkpoint
    private Boolean enableCheckpoint;

    //存放checkpoint的hdfs目录
    private String checkpointDataUri;


    /**
     * 根据命令执行参数，获取执行配置
     * @param args
     */
    public Boolean checkAndLoadExecParam(String[] args) {

        Boolean result = true;

        try {
            //ParameterTool
            ParameterTool paraTool = ParameterTool.fromArgs(args);


//            this.configPath = "/Users/liusc/Desktop/work/code/DataClean_Platform/src/main/java/com/dtdream/DataClean/config";
//            this.configPath = "C:\\MyProgram\\Project\\FlinkStudy\\src\\main\\java\\config";
//            this.parallelism = 1;
//            this.enableCheckpoint = false;



            this.configPath = paraTool.getRequired("configPath");
            this.parallelism = Integer.parseInt(paraTool.getRequired("parallelism"));
            this.enableCheckpoint = Boolean.valueOf(paraTool.getRequired("enableCheckpoint"));


            //使能checkpoint时，hdfs目录为必传参数
            if (this.enableCheckpoint) {
                this.checkpointDataUri = paraTool.get("checkpointDataUri");
            }
        } catch (Exception e) {
            e.printStackTrace();
            result = false;
        }

        return result;
    }


    public String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Boolean getEnableCheckpoint() {
        return enableCheckpoint;
    }

    public void setEnableCheckpoint(Boolean enableCheckpoint) {
        this.enableCheckpoint = enableCheckpoint;
    }

    public String getCheckpointDataUri() {
        return checkpointDataUri;
    }

    public void setCheckpointDataUri(String checkpointDataUri) {
        this.checkpointDataUri = checkpointDataUri;
    }
}

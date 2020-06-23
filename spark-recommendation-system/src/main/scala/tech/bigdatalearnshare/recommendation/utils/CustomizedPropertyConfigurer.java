package tech.bigdatalearnshare.recommendation.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @Author bigdatalearnshare
 * @Date 2018-05-13
 *
 * 通过重写spring PropertyPlaceholderConfigurer 的接口,实现外部加载配置文件
 */
public class CustomizedPropertyConfigurer extends PropertyPlaceholderConfigurer {

    private static Logger log = LoggerFactory.getLogger(CustomizedPropertyConfigurer.class);

    private String CONF_FILE_NAME = "conf";
    //主要用来读取外部配置文件
    private String ABS_PATH = new File("").getAbsolutePath().substring(0, new File("").getAbsolutePath().lastIndexOf("lib") == -1 ? 0 : new File("").getAbsolutePath().lastIndexOf("lib")) + this.CONF_FILE_NAME + File.separator;

    private Resource[] locations;

    @Override
    public void setLocations(Resource... locations) {
        super.setLocations(locations);
        this.locations = locations;
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

        Properties mergedProps = null;
        try {
            mergedProps = mergeProperties();
            for (Resource location : locations) {
                log.warn("SPRING_PROPERTIES_PATH_LOCAL=====" + location.getURL().getPath());
            }
        } catch (IOException ex) {
            mergedProps = new Properties();
            try {
                for (Resource location : locations) {
                    log.warn("SPRING_PROPERTIES_PATH_ABS=====" + this.ABS_PATH + location.getFilename());
                    mergedProps.load(new FileInputStream(this.ABS_PATH + location.getFilename()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        convertProperties(mergedProps);
        processProperties(beanFactory, mergedProps);
    }
}

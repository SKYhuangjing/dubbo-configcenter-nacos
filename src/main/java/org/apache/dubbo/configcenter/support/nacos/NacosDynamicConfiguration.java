/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.configcenter.support.nacos;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractSharedListener;
import com.alibaba.nacos.api.exception.NacosException;
import org.apache.commons.lang3.StringUtils;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.configcenter.ConfigChangeEvent;
import org.apache.dubbo.configcenter.ConfigChangeType;
import org.apache.dubbo.configcenter.ConfigurationListener;
import org.apache.dubbo.configcenter.DynamicConfiguration;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

/**
 * NacosMetadataReport
 */
public class NacosDynamicConfiguration implements DynamicConfiguration {

    private final static Logger logger = LoggerFactory.getLogger(NacosDynamicConfiguration.class);

    private final URL url;
    private final ConfigService configService;

    private ConcurrentMap<String, NacosConfigListener> listeners = new ConcurrentHashMap<>();


    public NacosDynamicConfiguration(URL url, ConfigService configService) {
        this.url = url;
        this.configService = configService;
    }

    private NacosConfigListener createTargetListener(String key, String group) {
        return new NacosConfigListener();
    }


    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        NacosConfigListener nacosConfigListener = listeners.computeIfAbsent(group + key, k -> createTargetListener(key, group));
        nacosConfigListener.addListener(listener);
        try {
            configService.addListener(key, group, nacosConfigListener);
        } catch (NacosException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        NacosConfigListener nacosConfigListener = listeners.get(group + key);
        if (nacosConfigListener != null) {
            nacosConfigListener.removeListener(listener);
        }
    }

    @Override
    public String getConfig(String key, String group, long timeout) throws IllegalStateException {
        try {
            return configService.getConfig(key, group, timeout);
        } catch (NacosException e) {
            logger.error("Failed to get config " + key + " to nacos " + ", cause: " + e.getMessage(), e);
            throw new IllegalStateException("Failed to get config " + key + " to nacos " + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getInternalProperty(String key) {
        try {
            return configService.getConfig(key, null, 5000);
        } catch (NacosException e) {
            logger.error("Failed to get config " + key + " to nacos " + ", cause: " + e.getMessage(), e);
            throw new IllegalStateException("Failed to get config " + key + " to nacos " + ", cause: " + e.getMessage(), e);
        }
    }

    public class NacosConfigListener extends AbstractSharedListener {

        private Set<ConfigurationListener> listeners = new CopyOnWriteArraySet<>();


        @Override
        public Executor getExecutor() {
            return null;
        }

        @Override
        public void innerReceive(String dataId, String group, String configInfo) {
            ConfigChangeEvent event = new ConfigChangeEvent(dataId, configInfo, getChangeType(configInfo));
            listeners.forEach(listener -> listener.process(event));
        }


        void addListener(ConfigurationListener configurationListener) {
            this.listeners.add(configurationListener);
        }

        void removeListener(ConfigurationListener configurationListener) {
            this.listeners.remove(configurationListener);
        }

        private ConfigChangeType getChangeType(String configInfo) {
            if (StringUtils.isBlank(configInfo)) {
                return ConfigChangeType.DELETED;
            }
            return ConfigChangeType.MODIFIED;
        }
    }
}

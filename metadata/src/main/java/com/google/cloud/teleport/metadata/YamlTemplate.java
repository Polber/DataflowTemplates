/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.metadata;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.lang.annotation.Annotation;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.EnumUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

public class YamlTemplate implements Template {

  private final String name;
  private final String displayName;
  private final String description;
  private final String category;
  private final String requirements;
  private final String flexContainerName;
  private final String yamlTemplateName;
  private final boolean hidden;
  private final String documentation;
  private final String contactInformation;
  private final boolean streaming;
  private final boolean preview;

  public YamlTemplate(
      String name,
      String displayName,
      String description,
      String requirements,
      String flexContainerName,
      String yamlTemplateName,
      String category,
      boolean hidden,
      String documentation,
      String contactInformation,
      boolean streaming,
      boolean preview) {
    checkArgument(!Strings.isNullOrEmpty(name), "name is required");
    checkArgument(!Strings.isNullOrEmpty(displayName), "displayName is required");
    checkArgument(!Strings.isNullOrEmpty(description), "description is required");
    checkArgument(!Strings.isNullOrEmpty(flexContainerName), "flex_container_name is required");
    checkArgument(!Strings.isNullOrEmpty(yamlTemplateName), "yaml_template_name is required");
    checkArgument(!Strings.isNullOrEmpty(category), "category is required");
    checkArgument(
        EnumUtils.isValidEnum(TemplateCategory.class, category),
        category + " is invalid category.");

    this.name = name;
    this.displayName = displayName;
    this.description = description;
    this.category = category;
    this.requirements = requirements;
    this.flexContainerName = flexContainerName;
    this.yamlTemplateName = yamlTemplateName;
    this.hidden = hidden;
    this.documentation = documentation;
    this.contactInformation = contactInformation;
    this.streaming = streaming;
    this.preview = preview;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public String displayName() {
    return this.displayName;
  }

  @Override
  public String[] description() {
    return new String[] {this.description};
  }

  @Override
  public String[] requirements() {
    return new String[] {this.requirements};
  }

  @Override
  public String flexContainerName() {
    return this.flexContainerName;
  }

  @Override
  public String yamlTemplateName() {
    return this.yamlTemplateName;
  }

  @Override
  public TemplateCategory category() {
    return TemplateCategory.valueOf(this.category);
  }

  @Override
  public boolean hidden() {
    return this.hidden;
  }

  @Override
  public String[] skipOptions() {
    throw new RuntimeException("skipOptions not implemented for Yaml templates.");
  }

  @Override
  public String[] optionalOptions() {
    throw new RuntimeException("optionalOptions not implemented for Yaml templates.");
  }

  @Override
  public Class<?> placeholderClass() {
    throw new RuntimeException("placeholderClass not implemented for Yaml templates.");
  }

  @Override
  public Class<?> optionsClass() {
    throw new RuntimeException("optionsClass not implemented for Yaml templates.");
  }

  @Override
  public Class<?>[] blocks() {
    return new Class[] {void.class};
  }

  @Override
  public Class<?> dlqBlock() {
    throw new RuntimeException("dlqBlock not implemented for Yaml templates.");
  }

  @Override
  public Class<?>[] optionsOrder() {
    throw new RuntimeException("optionsOrder not implemented for Yaml templates.");
  }

  @Override
  public String documentation() {
    return this.documentation;
  }

  @Override
  public String contactInformation() {
    return this.contactInformation;
  }

  @Override
  public AdditionalDocumentationBlock[] additionalDocumentation() {
    return new AdditionalDocumentationBlock[0];
  }

  @Override
  public TemplateType type() {
    return TemplateType.YAML;
  }

  @Override
  public boolean streaming() {
    return this.streaming;
  }

  @Override
  public boolean supportsAtLeastOnce() {
    return false;
  }

  @Override
  public boolean supportsExactlyOnce() {
    return false;
  }

  @Override
  public boolean preview() {
    return this.preview;
  }

  @Override
  public Class<? extends Annotation> annotationType() {
    throw new RuntimeException("annotationType not implemented for Yaml templates.");
  }
}

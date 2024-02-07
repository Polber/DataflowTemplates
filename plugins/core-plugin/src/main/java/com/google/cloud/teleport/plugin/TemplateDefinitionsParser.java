/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.plugin;

import com.google.cloud.teleport.metadata.MultiTemplate;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.YamlTemplate;
import com.google.cloud.teleport.plugin.model.ImageSpec;
import com.google.cloud.teleport.plugin.model.TemplateDefinitions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.reflections.Reflections;
import org.yaml.snakeyaml.Yaml;

/**
 * Utility class that will be used to scan for {@link Template} or {@link MultiTemplate} classes in
 * the classpath.
 */
public final class TemplateDefinitionsParser {

  private TemplateDefinitionsParser() {}

  /**
   * Given a ClassLoader, this method will scan look for every class that is annotated with {@link
   * Template} or {@link MultiTemplate}, by using {@link Reflections}. It then wraps all the
   * annotations and class name in a {@link TemplateDefinitions} instance, puts them in a list and
   * returns to the caller.
   *
   * @param classLoader ClassLoader that should be used to scan for the annotations.
   * @return Listed with all definitions that could be found in the classpath.
   */
  public static List<TemplateDefinitions> scanDefinitions(ClassLoader classLoader) {

    List<TemplateDefinitions> allDefinitions = new ArrayList<>();

    // Scan every @Template class
    Set<Class<?>> templates = new Reflections(classLoader).getTypesAnnotatedWith(Template.class);
    for (Class<?> templateClass : templates) {
      Template templateAnnotation = templateClass.getAnnotation(Template.class);
      allDefinitions.add(new TemplateDefinitions(templateClass, templateAnnotation));
    }

    // Scan every @MultiTemplate class
    Set<Class<?>> multiTemplates =
        new Reflections(classLoader).getTypesAnnotatedWith(MultiTemplate.class);
    for (Class<?> multiTemplateClass : multiTemplates) {
      MultiTemplate multiTemplateAnnotation = multiTemplateClass.getAnnotation(MultiTemplate.class);
      for (Template templateAnnotation : multiTemplateAnnotation.value()) {
        allDefinitions.add(new TemplateDefinitions(multiTemplateClass, templateAnnotation));
      }
    }

    // Do not return definitions that we have the subclass. Avoid duplications of the same template
    // for submodules / branded templates.
    List<TemplateDefinitions> filteredDefinitions = new ArrayList<>();
    for (TemplateDefinitions definitions : allDefinitions) {
      if (allDefinitions.stream()
          .noneMatch(
              other ->
                  definitions.getTemplateClass() != other.getTemplateClass()
                      && definitions
                          .getTemplateClass()
                          .isAssignableFrom(other.getTemplateClass()))) {
        filteredDefinitions.add(definitions);
      }
    }

    return filteredDefinitions;
  }

  public static void main(String[] args) throws IOException {
    List<TemplateDefinitions> templateDefinitions =
        scanYamlDefinitions("/Users/jkinard/DataflowTemplates/yaml/target/classes");
    for (TemplateDefinitions definition : templateDefinitions) {
      ImageSpec imageSpec = definition.buildSpecModel(false);
    }
  }

  public static List<TemplateDefinitions> scanYamlDefinitions(String directoryPath)
      throws IOException {
    File directory = new File(directoryPath);
    File[] yamlFiles = directory.listFiles((dir, name) -> name.toLowerCase().endsWith(".yaml"));

    List<TemplateDefinitions> definitions = new ArrayList<>();
    if (yamlFiles != null) {
      for (File yamlFile : yamlFiles) {
        Map<String, Object> yamlSpec = new Yaml().load(new FileInputStream(yamlFile));
        if (yamlSpec.containsKey("template")) {
          try {
            definitions.add(
                parseYamlDefinition(
                    (Map<String, Object>) yamlSpec.get("template"), yamlFile.getName()));
          } catch (Exception e) {
            throw new RuntimeException("Failed to parse template " + yamlFile.getName(), e);
          }
        }
      }
    }

    return definitions;
  }

  public static TemplateDefinitions parseYamlDefinition(Map<String, Object> yaml, String fileName) {
    TemplateDefinitions definition =
        new TemplateDefinitions(
            null,
            new YamlTemplate(
                yaml.getOrDefault("name", "").toString(),
                yaml.getOrDefault("display_name", "").toString(),
                yaml.getOrDefault("description", "").toString(),
                yaml.getOrDefault("requirements", "").toString(),
                yaml.getOrDefault("flex_container_name", "").toString(),
                fileName,
                yaml.getOrDefault("category", "").toString(),
                Boolean.parseBoolean(yaml.getOrDefault("hidden", "false").toString()),
                yaml.getOrDefault("documentation", "").toString(),
                yaml.getOrDefault("contactInformation", "").toString(),
                Boolean.parseBoolean(yaml.getOrDefault("streaming", "false").toString()),
                Boolean.parseBoolean(yaml.getOrDefault("preview", "false").toString())));

    return definition;
  }

  /**
   * Parse the version of a template, given the stage prefix.
   *
   * @param stagePrefix GCS path to store the templates (e.g., templates/2023-03-03_RC00).
   * @return Only the last part, after replacing characters not allowed in labels.
   */
  public static String parseVersion(String stagePrefix) {
    String[] parts = stagePrefix.split("/");
    String lastPart = parts[parts.length - 1];

    // Replace not allowed chars (anything other than letters, digits, hyphen and underscore)
    return lastPart.toLowerCase().replaceAll("[^\\p{Ll}\\p{Lo}\\p{N}_-]", "_");
  }
}

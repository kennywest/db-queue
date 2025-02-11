package ru.yandex.money.common.dbqueue;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.ImportOptions;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * @author Oleg Kandaurov
 * @since 03.08.2017
 */
public class ArchitectureTest {

    private static final String BASE_PACKAGE = "ru.yandex.money.common.dbqueue";

    private JavaClasses classes;

    @Before
    public void importClasses() {
        classes = new ClassFileImporter(new ImportOptions()
                .with(new ImportOption.DontIncludeTests()))
                .importPackages(BASE_PACKAGE);
    }

    @Test
    public void test1() {
        ArchRule rule = noClasses().that().resideInAnyPackage(
                fullNames("api..", "dao..", "internal..", "settings..", "init.."))
                .should().accessClassesThat().resideInAnyPackage(fullNames("spring"))
                .because("spring context is optional dependency");
        rule.check(classes);
    }

    @Test
    public void test2() {
        ArchRule rule = classes().that().resideInAnyPackage(
                fullNames("api"))
                .should().accessClassesThat().resideInAnyPackage(fullNames("api", "settings"))
                .orShould().accessClassesThat().resideInAnyPackage("java..")
                .because("api must not depend on implementation details");
        rule.check(classes);
    }

    @Test
    public void test3() {
        ArchRule rule = classes().that().resideInAnyPackage(
                fullNames("settings"))
                .should().accessClassesThat().resideInAnyPackage(fullNames("settings"))
                .orShould().accessClassesThat().resideInAnyPackage("java..")
                .because("settings must not depend on implementation details");
        rule.check(classes);
    }

    @Test
    public void test4() {
        ArchRule rule = noClasses().that().resideInAnyPackage(
                fullNames("settings..", "api..", "dao..", "spring.."))
                .should().accessClassesThat().resideInAnyPackage(fullNames("internal.."))
                .because("public classes must not depend on internal details");
        rule.check(classes);
    }

    @Test
    public void test5() {
        ArchRule rule = noClasses().that().resideInAnyPackage(
                fullNames("settings..", "api", "init.."))
                .should().accessClassesThat().resideInAnyPackage("org.springframework..")
                .because("api classes must not depend on spring");
        rule.check(classes);
    }

    @Test
    public void test6() {
        ArchRule rule = classes().that().resideInAnyPackage(
                fullNames("dao..", "internal.."))
                .should().accessClassesThat().resideInAnyPackage("org.springframework.jdbc..", "java.lang..")
                .orShould().accessClassesThat().resideInAnyPackage(fullNames("dao..", "api..", "settings..", "internal.."))
                .because("public classes must depend only on spring jdbc");
        rule.check(classes);
    }


    private static String[] fullNames(String... relativeName) {
        return Arrays.stream(relativeName).map(name -> BASE_PACKAGE + "." + name)
                .collect(Collectors.toList()).toArray(new String[relativeName.length]);
    }


}

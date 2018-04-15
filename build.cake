#addin "nuget:?package=Cake.FileHelpers&version=2.0.0"

//////////////////////////////////////////////////////////////////////
// ARGUMENTS
//////////////////////////////////////////////////////////////////////
var target = Argument("Target", "Default");
var configuration = Argument("configuration", "Release");
var buildCounter = Argument("buildCounter", "0"); // "version" is reserved

//////////////////////////////////////////////////////////////////////
// CONSTANTS
//////////////////////////////////////////////////////////////////////

var artifactsDirectory = Directory("./artifacts");
var tempDirectory = Directory("./temp");

var semVer = FileReadText("./semver.txt");
var buildVersion = semVer.Contains("-")
    ? semVer + buildCounter
	: semVer;

//////////////////////////////////////////////////////////////////////
// TASKS
//////////////////////////////////////////////////////////////////////

Task("Clean")
    .Does(() =>
{
    EnsureDirectoryExists(artifactsDirectory);
    EnsureDirectoryExists(tempDirectory);

    CleanDirectory(artifactsDirectory);
    CleanDirectory(tempDirectory);

    DotNetCoreClean("./src/PeregrineDb", new DotNetCoreCleanSettings { Configuration = configuration });
});

Task("Restore")
    .IsDependentOn("Clean")
    .Does(() =>
{
    DotNetCoreRestore();
});

Task("Build")
    .IsDependentOn("Clean")
    .IsDependentOn("Restore")
    .Does(() =>
{
    RunProcess("dotnet", new ProcessSettings
        {
            WorkingDirectory = Directory("."),
            Arguments = $@"build -c {configuration} --no-restore /p:Version={buildVersion}"
        });
});

Task("Test")
    .IsDependentOn("Build")
    .Does(() =>
{
    RunProcess("dotnet", new ProcessSettings
        {
            WorkingDirectory = Directory("./tests/PeregrineDb.Tests"),
            Arguments = $@"test -c {configuration} --no-build --no-restore /p:Version={buildVersion}"
        });
});

Task("Pack")
    .IsDependentOn("Test")
    .Does(() =>
{
    RunProcess("dotnet", new ProcessSettings
        {
            WorkingDirectory = Directory("."),
            Arguments = $@"pack -c {configuration} -o ""{MakeAbsolute(artifactsDirectory).FullPath}"" --no-build /p:PackageVersion={buildVersion}"
        });
});

//////////////////////////////////////////////////////////////////////
// TASK METHODS
//////////////////////////////////////////////////////////////////////

void RunProcess(string name, ProcessSettings settings)
{
    var exitCode = StartProcess(name, settings);
    if (exitCode != 0)
    {
        throw new InvalidOperationException($"Unexpected error code from {name}: {exitCode}");
    }
}

//////////////////////////////////////////////////////////////////////
// TASK TARGETS
//////////////////////////////////////////////////////////////////////

Task("Default")
    .IsDependentOn("Pack");

Task("CommitTest")
    .IsDependentOn("Test");

//////////////////////////////////////////////////////////////////////
// EXECUTION
//////////////////////////////////////////////////////////////////////

RunTarget(target);
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Raven.Server.Documents.Indexes.Static.NuGet
{
    public static class NuGetNativeLibraryResolver
    {
        private const uint LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000;

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        static extern bool SetDefaultDllDirectories(uint flags);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern int AddDllDirectory(string path);

        private static readonly object _locker = new object();

        private static readonly HashSet<string> _registeredPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private static readonly Dictionary<Assembly, bool> _registeredAssemblies = new Dictionary<Assembly, bool>();

        private static readonly Dictionary<string, string> _nativeLibraries = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        public static void RegisterPath(string path)
        {
            if (path == null)
                return;

            if (Directory.Exists(path) == false)
                return;

            lock (_locker)
            {
                //if (_registeredPaths.Count == 0)
                //    SetDefaultDllDirectories(LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);

                if (_registeredPaths.Add(path) == false)
                    return;

                //var result = AddDllDirectory(path);

                foreach (var file in Directory.GetFiles(path))
                {
                    var nativeLibrary = Path.GetFileNameWithoutExtension(file);
                    _nativeLibraries[nativeLibrary] = path;
                }
            }
        }

        public static void RegisterAssembly(Assembly assembly)
        {
            if (assembly == null)
                return;

            lock (_locker)
            {
                _registeredAssemblies.TryAdd(assembly, false);
            }
        }

        public static void EnsureAssembliesRegisteredToNativeLibraries()
        {
            lock (_locker)
            {
                if (_nativeLibraries.Count == 0 || _registeredAssemblies.Count == 0)
                    return;

                foreach (var kvp in _registeredAssemblies)
                {
                    var assembly = kvp.Key;
                    var registered = kvp.Value;

                    if (registered)
                        continue;

                    NativeLibrary.SetDllImportResolver(assembly, Resolver);
                    _registeredAssemblies[assembly] = true;
                }
            }
        }

        private static IntPtr Resolver(string libraryName, Assembly assembly, DllImportSearchPath? dllImportSearchPath)
        {
            if (_nativeLibraries.TryGetValue(libraryName, out var path) == false)
                return IntPtr.Zero;

            if (Directory.Exists(path) == false)
                return IntPtr.Zero;

            var libraryPath = Path.Combine(path, libraryName + ".dll");
            if (File.Exists(libraryPath) == false)
                return IntPtr.Zero;

            return NativeLibrary.Load(libraryPath);
        }
    }
}

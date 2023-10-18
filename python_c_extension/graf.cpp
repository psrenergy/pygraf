#include <cassert>
#include <cstdio>
#include <string>
#include <vector>
#include <Python.h>

typedef float data_t;

PyObject * create();
PyObject * destroy(PyObject *, PyObject *);
PyObject * load_header(PyObject *, PyObject *);
PyObject * open_bin(PyObject *, PyObject *);
PyObject * close(PyObject *, PyObject *);
PyObject * read(PyObject *, PyObject *);
PyObject * blocks(PyObject *, PyObject *);
PyObject * agents(PyObject *, PyObject *);


char grafcmod_docs[] = "_grafc module.";
char createfunc_docs[] = "create function.";
char destroyfunc_docs[] = "destroy function.";
char load_headerfunc_docs[] = "load_header function.";
char open_binfunc_docs[] = "load_bin function.";
char closefunc_docs[] = "close function.";
char readfunc_docs[] = "read function.";
char blocksfunc_docs[] = "blocks function.";
char agentsfunc_docs[] = "agents function.";


PyMethodDef grafc_funcs[] = {
  {
    "create",
    (PyCFunction)create,
    METH_NOARGS,
    createfunc_docs
  },
  {
    "destroy",
    (PyCFunction)destroy,
    METH_VARARGS,
    destroyfunc_docs
  },
  {
    "close",
    (PyCFunction)close,
    METH_VARARGS,
    closefunc_docs
  },
  {
    "load_header",
    (PyCFunction)load_header,
    METH_VARARGS,
    load_headerfunc_docs
  },
  {
    "open_bin",
    (PyCFunction)open_bin,
    METH_VARARGS,
    open_binfunc_docs
  },
  {
    "read",
    (PyCFunction)read,
    METH_VARARGS,
    readfunc_docs
  },
  {
    "blocks",
    (PyCFunction)blocks,
    METH_VARARGS,
    blocksfunc_docs
  },
  {
    "agents",
    (PyCFunction)agents,
    METH_VARARGS,
    agentsfunc_docs
  },

  {	nullptr}
};


PyModuleDef grafc_mod = {
  PyModuleDef_HEAD_INIT,
  "_grafc",
  grafcmod_docs,
  -1,
  grafc_funcs,
  nullptr,
  nullptr,
  nullptr,
  nullptr
};


PyMODINIT_FUNC PyInit__grafc()
{
  return PyModule_Create(&grafc_mod);
}


constexpr int WORD_SIZE = 4;


enum class BlockType
{
    Block = 0,
    Hour = 1,
};


enum class StageType
{
  Weekly = 1,
  Monthly = 2,
};


struct HdrBinFile
{
  int initial_stage;
  int stages;
  int min_stage;
  int max_stage;
  int scenarios;
  int case_initial_stage;
  bool varies_by_scenario;
  bool varies_by_block;
  BlockType hour_or_block;
  StageType stage_type;
  int initial_year;
  std::string units;
  std::vector<std::string> agents;
  std::vector<int> offsets;

  std::string current_file;
  std::string encoding;
  FILE* file;
  std::streamoff binoffset;
  std::vector<data_t> current_data;

  HdrBinFile();
  ~HdrBinFile();

  bool open_file(std::string const & file_path);
  bool open_bin_file(std::string const & file_path);
  void close_file();
  int blocks(int stage);
  void seek(int stage, int scenario, int block);
  bool read_data(int stage, int scenario, int block);
  bool read_header(std::string const & file_path);
};


HdrBinFile::HdrBinFile()
  : initial_stage(0), stages(0), min_stage(0), max_stage(0),
    scenarios(0), case_initial_stage(0), varies_by_scenario(false),
    varies_by_block(false), hour_or_block(BlockType::Block),
    stage_type(StageType::Monthly), initial_year(0), binoffset(0),
    encoding{"latin1"}, file(nullptr)
{
}

HdrBinFile::~HdrBinFile()
{
  if (file != nullptr)
  {
    close_file();
  }
}

std::string get_strerror(int errnum)
{
  size_t const maxsize = 1024;
  std::string buf(maxsize, '\0');
#if ((_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE)
  // XSI-compliant version
  auto result = strerror_r(errnum, &buf[0], maxsize);
  assert(result != 0);
  buf.resize(strlen(buf.c_str()));
  return buf;

#elif _GNU_SOURCE
  // GNU-specific version
  auto msg = strerror_r(errnum, &buf[0], maxsize);
  assert(msg == buf.c_str() && strlen(buf.c_str()) == maxsize -1 && "buffer probably full");
  buf.resize(strlen(buf.c_str()));
  return msg;

#elif __MINGW32__
  auto cptr = strerror(errnum);
  return cptr;
#elif _WIN32
  auto result = strerror_s(&buf[0], maxsize, errnum);
  assert(result == 0 && std::strlen(buf.c_str()) == maxsize - 1 && "buffer probably is full");
  buf.resize(strlen(buf.c_str()));
  return buf;
#else
  auto result = strerror_r(errnum, &buf[0], maxsize);
  if (result == -1)
  buf.resize(strlen(buf.c_str()));
  return buf;
#endif

}


void read_set_exception(size_t size, char const * file_path)
{
  if (errno != 0)
  {
    auto const error_msg = get_strerror(errno);
    PyErr_Format(PyExc_EOFError, "could not read %zu bytes from file %s: %s",
                 size, file_path, error_msg.c_str());
  }
  else
  {
    PyErr_Format(PyExc_EOFError, "could not read %zu bytes from file %s",
                 size, file_path);
  }
}

bool read_or_raise(FILE *file, void *var, size_t size, char const * file_path)
{
  size_t n = fread(var, 1, size, file);
  if (n != size)
  {
    if (ferror(file) || feof(file))
    {
      read_set_exception(size, file_path);
      return false;
    }
    else
    {
      assert(0 && "unreachable");
    }
  }
  return true;
}

#define READ_SIZE(file, var, size) do {      \
  if (!read_or_raise((file), &(var), (size), current_file.c_str())) \
    return false;                             \
  } while(false)

#define READ(file, var) READ_SIZE(file, var, sizeof(var))


std::string trim( std::string const& str )
{
  static char const* whitespaceChars = "\n\r\t ";
  std::string::size_type start = str.find_first_not_of( whitespaceChars );
  std::string::size_type end = str.find_last_not_of( whitespaceChars );

  return start != std::string::npos ? str.substr( start, 1+end-start ) : "";
}


bool HdrBinFile::open_file(std::string const & file_path)
{
  file = fopen(file_path.c_str(), "rb");
  if (file == nullptr)
  {
    auto const msg = get_strerror(errno);
    PyErr_Format(PyExc_IOError, "could not open file %s: %s",
                 file_path.c_str(), msg.c_str());
    return false;
  }
  return true;
}

bool HdrBinFile::open_bin_file(std::string const & file_path)
{
  // Close header
  close_file();

  // Open bin from beggining.
  binoffset = 0;
  file = fopen(file_path.c_str(), "rb");
  if (file == nullptr)
  {
    auto const msg = get_strerror(errno);
    PyErr_Format(PyExc_IOError, "could not open file %s: %s",
                 file_path.c_str(), msg.c_str());
    return false;
  }
  current_file = file_path;
  return true;
}


void HdrBinFile::close_file()
{
  fclose(file);
  file = nullptr;
}


void HdrBinFile::seek(int stage, int scenario, int block)
{
  // Move file stream to the desired index.
  // Scenario and block are 1-based. Stage is 0-based.
  auto index = (offsets[stage] * scenarios
    + blocks(stage + 1) * (scenario - 1)
    + (block - 1)) * agents.size();
  std::streamoff offset_from_start = binoffset + index * WORD_SIZE;
  fseek(file, offset_from_start, SEEK_SET);
}


int HdrBinFile::blocks(int stage)
{
  // Number of blocks of a given stage. 1-based stage number.
  if (varies_by_block)
  {
    int istage = stage - min_stage + 1;
    return offsets[istage] - offsets[istage - 1];
  }
  return 1;
}


bool HdrBinFile::read_data(int stage, int scenario, int block)
{
  // stage, scenario, and block are 1-based indexes.
  assert(stage >= 0);
  assert(scenario >= 0);
  assert(block >= 0);
  // TODO: check scenarios.
  auto istage = stage - min_stage;
  seek(istage, scenario, block);
  current_data.resize(agents.size());
  READ_SIZE(file, current_data[0], current_data.size() * sizeof(data_t));
  return true;
}


bool HdrBinFile::read_header(std::string const & file_path)
{
  // TODO: version == 1 is unsupported.
  current_file = file_path;
  int ignored;
  int version;
  int agents_total;
  int hdr_hour_or_block;
  int hdr_stage_type;
  int hdr_varies_by_scenario;
  int hdr_varies_by_block;
  units.resize(8);
  int maxname;
  READ(file, ignored);
  READ(file, version);
  READ(file, ignored);
  READ(file, ignored);
  READ(file, this->min_stage);
  READ(file, this->max_stage);
  READ(file, this->scenarios);
  READ(file, agents_total);
  READ(file, hdr_varies_by_scenario);
  READ(file, hdr_varies_by_block);
  READ(file, hdr_hour_or_block);
  READ(file, hdr_stage_type);
  READ(file, this->case_initial_stage);
  READ(file, this->initial_year);
  READ_SIZE(file, units[0], sizeof(units.c_str()) - 1);
  units[7] = '\0';
  READ(file, maxname);

  this->varies_by_scenario = hdr_varies_by_scenario == 1;
  this->varies_by_block = hdr_varies_by_block == 1;

  this->stages = this->max_stage - this->min_stage + 1;
  if (hdr_hour_or_block == 0)
  {
    this->hour_or_block = BlockType::Block;
  }
  else
  {
    this->hour_or_block = BlockType::Hour;
  }

  switch (hdr_stage_type)
  {
    case 1:
      this->stage_type = StageType::Weekly;
      break;
    default:
    case 2:
      this->stage_type = StageType::Monthly;
      break;
  }

  if (this->varies_by_scenario == 0)
  {
    this->scenarios = 1;
  }

  // Offsets
  READ(file, ignored);
  READ(file, ignored);
  this->offsets.resize(this->stages + 1);
  for (int i = 0; i < this->stages + 1; ++i)
  {
    READ(file, this->offsets[i]);
  }
  READ(file, ignored);

  this->agents.reserve(agents_total);
  for (int i = 0; i < agents_total; ++i)
  {
    int length;
    READ(file, length);
    this->agents.emplace_back(length, '\0');
    std::string & agent_name = this->agents[i];
    READ_SIZE(file, agent_name[0], length);
    READ(file, ignored);
  }
  this->binoffset = ftell(this->file);
  return true;
}


PyObject * create()
{
  // Store HdrBinFile pointer into pyobject and return it as long integer.
  auto file = new HdrBinFile();
  return PyLong_FromVoidPtr(file);
}

PyObject * destroy(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  if(!PyArg_ParseTuple(args, "L", &ptr))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  delete file;

  Py_RETURN_NONE;
}

PyObject * load_header(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  char * path;
  if(!PyArg_ParseTuple(args, "Ls", &ptr, &path))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  if (!file->open_file(path))
  {
    return nullptr;
  }
  if (!file->read_header(path))
  {
    return nullptr;
  }

  // Returns header data as Python tuple.
  PyObject * tuple = PyTuple_New(12);
  PyTuple_SetItem(tuple, 0, PyLong_FromLong(file->initial_stage));
  PyTuple_SetItem(tuple, 1, PyLong_FromLong(file->stages));
  PyTuple_SetItem(tuple, 2, PyLong_FromLong(file->min_stage));
  PyTuple_SetItem(tuple, 3, PyLong_FromLong(file->max_stage));
  PyTuple_SetItem(tuple, 4, PyLong_FromLong(file->scenarios));
  PyTuple_SetItem(tuple, 5, PyLong_FromLong(file->case_initial_stage));
  PyTuple_SetItem(tuple, 6, PyLong_FromLong(file->varies_by_scenario));
  PyTuple_SetItem(tuple, 7, PyLong_FromLong(file->varies_by_block));
  PyTuple_SetItem(tuple, 8, PyLong_FromLong(static_cast<int>(file->hour_or_block)));
  PyTuple_SetItem(tuple, 9, PyLong_FromLong(static_cast<int>(file->stage_type)));
  PyTuple_SetItem(tuple, 10, PyLong_FromLong(file->initial_year));
  PyTuple_SetItem(tuple, 11, PyUnicode_FromString(file->units.c_str()));
  return tuple;
}

PyObject * open_bin(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  char * path;
  if(!PyArg_ParseTuple(args, "Ls", &ptr, &path))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  if (!file->open_bin_file(path))
  {
    return nullptr;
  }

  Py_RETURN_NONE;
}

PyObject * close(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  if(!PyArg_ParseTuple(args, "L", &ptr))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  file->close_file();
  Py_RETURN_NONE;
}

PyObject * read(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  int stage, scenario, block;
  if(!PyArg_ParseTuple(args, "Liii", &ptr, &stage, &scenario, &block))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);

  if (!file->read_data(stage, scenario, block))
  {
    return nullptr;
  }

  PyObject * tuple = PyTuple_New(file->current_data.size());
  for (size_t i = 0; i < file->current_data.size(); ++i)
  {
    PyTuple_SetItem(tuple, i, PyFloat_FromDouble(file->current_data[i]));
  }
  return tuple;
}

PyObject * blocks(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  int stage;
  if(!PyArg_ParseTuple(args, "Li", &ptr, &stage))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  return PyLong_FromLong(file->blocks(stage));
}

PyObject * agents(PyObject * self, PyObject * args)
{
  (void) self;
  long long ptr;
  if(!PyArg_ParseTuple(args, "L", &ptr))
    return nullptr;

  auto file = reinterpret_cast<HdrBinFile *>(ptr);
  // Return agents' names as Python tuple of string.
  PyObject * tuple = PyTuple_New(file->agents.size());
  for (size_t i = 0; i < file->agents.size(); ++i)
  {
    auto const& agent = trim(file->agents[i]);
    PyTuple_SetItem(tuple, i, PyUnicode_Decode(agent.c_str(),
        agent.size(), file->encoding.c_str(), "strict"));
  }
  return tuple;
}

SRCS = main.cc
EMU_OBJS = $(subst .cc,.emu.o,$(SRCS))

#EMU_PATH = /local/devel/packages/emu-18.11-cplus
EMU_PATH = /local/devel/packages/emu-19.02
#EMU_PATH = /home/jgwohlbier/devel/packages/emu-19.02
EMU_CXX = $(EMU_PATH)/bin/emu-cc
EMU_SIM = $(EMU_PATH)/bin/emusim.x
EMU_SIM_OPTS = --short_trace

EXE  = stdvector
EMU_EXE = $(EXE).mwx

LDFLAGS = -lemu_c_utils

$(EMU_EXE) : $(EMU_OBJS)
	$(EMU_CXX) -o $(EMU_EXE) $(EMU_OBJS) $(LDFLAGS)

run : $(EMU_EXE)
	$(EMU_SIM) $(EMU_SIM_OPTS) $(EMU_EXE)

%.emu.o: %.cc
	$(EMU_CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

.PHONY : clean

clean :
	-$(RM) *~ $(OBJS) $(EMU_OBJS) $(EXE) $(EMU_EXE) *.cdc *.hdd *.vsf

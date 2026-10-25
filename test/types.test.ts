import { expectTypeOf, it } from "vitest";
import type * as Pi from "@earendil-works/pi-ai";
import type * as Mule from "../src/types.js";

it("keeps message and tool data structural and owned models assignable to Pi", () => {
  expectTypeOf<Mule.Message>().toExtend<Pi.Message>();
  expectTypeOf<Pi.Message>().toExtend<Mule.Message>();
  expectTypeOf<Mule.Tool>().toExtend<Pi.Tool>();
  expectTypeOf<Pi.Tool>().toExtend<Mule.Tool>();
  expectTypeOf<Mule.Model<Mule.Api>>().toExtend<Pi.Model<Pi.Api>>();
  expectTypeOf<Mule.AssistantMessageEvent>().toExtend<Pi.AssistantMessageEvent>();
  expectTypeOf<Pi.AssistantMessageEvent>().toExtend<Mule.AssistantMessageEvent>();
  expectTypeOf<Pi.AssistantMessageEventStream>().toExtend<Mule.AssistantMessageEventStream>();
});

/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 NEM
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

import {deepEqual} from "assert";
import {expect} from "chai";
import {Address} from "../../../src/models/account/Address";
import {PublicAccount} from "../../../src/models/account/PublicAccount";
import {Asset} from "../../../src/models/asset/Asset";
import {AssetDefinition, AssetProperties} from "../../../src/models/asset/AssetDefinition";
import {AssetId} from "../../../src/models/asset/AssetId";
import {AssetLevy, AssetLevyType} from "../../../src/models/asset/AssetLevy";
import {AssetTransferable} from "../../../src/models/asset/AssetTransferable";
import {NetworkTypes} from "../../../src/models/node/NetworkTypes";
import {NEMLibrary} from "../../../src/NEMLibrary";

describe("AssetTransferable", () => {
  before(() => {
    NEMLibrary.bootstrap(NetworkTypes.TEST_NET);
  });

  after(() => {
    NEMLibrary.reset();
  });

  it("should create a asset transferable object", () => {
    const namespaceId = "nem";
    const name = "coin";
    const quantity = 10;

    const assetTransferable = new AssetTransferable(new AssetId(namespaceId, name), new AssetProperties(0, 1000, true, false), quantity);
    expect(assetTransferable.assetId.namespaceId).to.be.equal(namespaceId);
    expect(assetTransferable.assetId.name).to.be.equal(name);
    expect(assetTransferable.relativeQuantity()).to.be.equal(quantity);
    expect(assetTransferable.levy).to.be.undefined;
    expect(assetTransferable.properties.initialSupply).to.be.equal(1000);
    expect(assetTransferable.properties.divisibility).to.be.equal(0);
    expect(assetTransferable.properties.transferable).to.be.true;
    expect(assetTransferable.properties.supplyMutable).to.be.false;
  });

  it("should create a asset transferable object with levy", () => {
    const quantity = 10;
    const assetId = new AssetId("nem", "xem");
    const levy = new AssetLevy(AssetLevyType.Absolute, new Address("TCJZJHAV63RE2JSKN27DFIHZRXIHAI736WXEOJGA"), new AssetId("nem", "xem"), 1);

    const assetTransferable = new AssetTransferable(assetId, new AssetProperties(0, 1000, true, false), quantity, levy);
    deepEqual(assetTransferable.assetId, assetId);
    expect(assetTransferable.relativeQuantity()).to.be.equal(quantity);
    expect(assetTransferable.properties.initialSupply).to.be.equal(1000);
    expect(assetTransferable.properties.divisibility).to.be.equal(0);
    expect(assetTransferable.properties.transferable).to.be.true;
    expect(assetTransferable.properties.supplyMutable).to.be.false;
    deepEqual(assetTransferable.levy, levy);
  });

  it("should create a assetDefinition object", () => {
    const creator = PublicAccount.createWithPublicKey("a4f9d42cf8e1f7c6c3216ede81896c4fa9f49071ee4aee2a4843e2711899b23a");
    const id = new AssetId("nem", "coin");
    const description = "assetDescription";
    const properties = new AssetProperties(0, 1000, true, false);
    const levy: AssetLevy = new AssetLevy(1, new Address("TCJZJHAV63RE2JSKN27DFIHZRXIHAI736WXEOJGA"), id, 1000000);

    const assetDefinition = new AssetDefinition(creator, id, description, properties, levy);
    const assetTransferable = AssetTransferable.createWithAssetDefinition(assetDefinition, 10);

    deepEqual(assetTransferable.assetId, id);
    expect(assetTransferable.relativeQuantity()).to.be.equal(10);
    expect(assetTransferable.properties.transferable).to.be.equal(true);
    expect(assetTransferable.properties.supplyMutable).to.be.equal(false);
    expect(assetTransferable.properties.initialSupply).to.be.equal(1000);
    expect(assetTransferable.properties.divisibility).to.be.equal(0);
    deepEqual(assetTransferable.levy, levy);

  });

});

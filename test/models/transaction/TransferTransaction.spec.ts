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
import {Account} from "../../../src/models/account/Account";
import {Address} from "../../../src/models/account/Address";
import {PublicAccount} from "../../../src/models/account/PublicAccount";
import {AssetProperties} from "../../../src/models/asset/AssetDefinition";
import {AssetId} from "../../../src/models/asset/AssetId";
import {AssetTransferable} from "../../../src/models/asset/AssetTransferable";
import {XEM} from "../../../src/models/asset/XEM";
import {NetworkTypes} from "../../../src/models/node/NetworkTypes";
import {EmptyMessage, PlainMessage} from "../../../src/models/transaction/PlainMessage";
import {TimeWindow} from "../../../src/models/transaction/TimeWindow";
import {HashData, TransactionInfo} from "../../../src/models/transaction/TransactionInfo";
import {TransactionTypes} from "../../../src/models/transaction/TransactionTypes";
import {TransferTransaction} from "../../../src/models/transaction/TransferTransaction";
import {NEMLibrary} from "../../../src/NEMLibrary";
import {TestVariables} from "../../config/TestVariables.spec";

declare let process: any;

describe("TransferTransaction", () => {
  const PUBLIC_KEY = "5d5d829644625eb6554273f70b1187d904761fab4c5c0e5f01666f6725e9278b";
  const privateKey: string = process.env.PRIVATE_KEY || TestVariables.TEST_PRIVATE_KEY;
  let recipientPublicAccount: PublicAccount;

  before(() => {
    NEMLibrary.bootstrap(NetworkTypes.TEST_NET);
    recipientPublicAccount = PublicAccount.createWithPublicKey("b254d8b2b00e1b1266eb54a6931cd7c1b0f307e41d9ebb01f025f4933758f0be");
  });

  after(() => {
    NEMLibrary.reset();
  });

  it("should create a TransferTransaction", () => {
    const amount = 3;
    const transferTransaction = new TransferTransaction(
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      XEM.fromRelative(amount),
      TimeWindow.createWithDeadline(),
      1744830466,
      4242424,
      EmptyMessage);
    transferTransaction.signer = PublicAccount.createWithPublicKey(PUBLIC_KEY);
    expect(transferTransaction.type).to.be.equal(TransactionTypes.TRANSFER);
    expect(transferTransaction.signer.publicKey).to.be.equal(PUBLIC_KEY);
    expect(transferTransaction.recipient.pretty()).to.be.equal("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA");
    expect(transferTransaction.xem().relativeQuantity()).to.be.equal(amount);
    expect(transferTransaction.xem().quantity).to.be.equal(amount * 1e6);
    expect(transferTransaction.xem().absoluteQuantity()).to.be.equal(amount * 1e6);
    expect(transferTransaction.fee).to.be.equal(4242424);
    expect(transferTransaction.version).to.be.equal(1744830466);
  });

  it("should return is not confirmed when there's no transaction info", () => {
    const transferTransaction = new TransferTransaction(
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(30000),
      TimeWindow.createWithDeadline(),
      1744830466,
      4242424,
      EmptyMessage);
    expect(transferTransaction.isConfirmed()).to.be.false;
  });

  it("should throw Error when it is not confirmed and we call getTransactionInfo", () => {
    const transferTransaction = new TransferTransaction(
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(30000),
      TimeWindow.createWithDeadline(),
      1744830466,
      4242424,
      EmptyMessage);
    expect(transferTransaction.getTransactionInfo).to.throw();
  });

  it("should return is confirmed when there's transaction info", () => {
    const transactionInfo = new TransactionInfo(12, 13, {data: "my hash"});
    const transferTransaction = new TransferTransaction(
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(30000),
      TimeWindow.createWithDeadline(),
      1744830466,
      4242424,
      EmptyMessage,
      undefined,
      undefined,
      undefined,
      transactionInfo,
    );
    expect(transferTransaction.isConfirmed()).to.be.true;
  });

  it("should return TransactionInfo when it is confirmed and we call getTransactionInfo", () => {
    const transactionInfo = new TransactionInfo(12, 13, {data: "my hash"});
    const transferTransaction = new TransferTransaction(
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(30000),
      TimeWindow.createWithDeadline(),
      1744830466,
      4242424,
      EmptyMessage,
      undefined,
      undefined,
      undefined,
      transactionInfo,
    );
    deepEqual(transferTransaction.getTransactionInfo(), transactionInfo);
  });

  it("should be created by the named constructor", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(3),
      EmptyMessage);
    expect(transferTransaction.xem().relativeQuantity()).to.be.equal(3);
    expect(transferTransaction.xem().quantity).to.be.equal(3 * 1e6);
    expect(transferTransaction.xem().absoluteQuantity()).to.be.equal(3 * 1e6);
    expect(transferTransaction.version).to.be.equal(1);
  });

  it("should create a transfer transaction with a mosaic and have the version 2", () => {
    const transaction = TransferTransaction.createWithAssets(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      [new AssetTransferable(new AssetId("multisigns", "mosaic"), new AssetProperties(), 4000)],
      EmptyMessage,
    );
    expect(transaction.version).to.be.equal(2);
    deepEqual(transaction.assets()[0].assetId, new AssetId("multisigns", "mosaic"));
  });

  it("should have the minimum fee calculated 0 xem transaction for TEST_NET", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(0),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(0.05 * 1000000));
  });

  it("should have minimum fee with a message for TEST_NET", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(0),
      PlainMessage.create("test transaction"));
    expect(transferTransaction.fee).to.be.equal(Math.floor(0.05 * 1000000) + 0.05 * 1000000);
  });

  it("should take into account the maximum of fee for given an xem of less 19000", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(19999.9999),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(new XEM(0.05).absoluteQuantity()));
  });

  it("should take into account the maximum of fee for given an xem of less 19000", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(20000),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(new XEM(0.10).absoluteQuantity()));
  });

  it("should take into account the maximum of fee for given a huge xem", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(250000),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(new XEM(1.25).absoluteQuantity()));
  });

  it("should take into account the maximum of fee for given a huge xem -1", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(249999),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(new XEM(1.20).absoluteQuantity()));
  });

  it("should take into account the maximum of fee for given a huge xem", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(500000),
      EmptyMessage);
    expect(transferTransaction.fee).to.be.equal(Math.floor(new XEM(1.25).absoluteQuantity()));
  });

  describe("should get the same fee sending regular xem transaction and mosaic transaction xem 50", () => {
    it("v1", () => {
      const transferTransaction = TransferTransaction.create(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        new XEM(50),
        EmptyMessage);
      expect(transferTransaction.fee).to.be.equal(0.05 * 1e6);
    });

    it("v2", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new XEM(50)],
        EmptyMessage,
      );
      expect(mosaicTransferTransaction.fee).to.be.equal(0.05 * 1e6);
    });
  });

  describe("should get the same fee sending regular xem transaction and mosaic transaction xem 500000", () => {
    const xem = XEM.fromRelative(500000);

    it("v1", () => {
      const transferTransaction = TransferTransaction.create(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        xem,
        EmptyMessage);
      expect(transferTransaction.fee).to.be.equal(1.25 * 1e6);
    });

    it("v2", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [xem],
        EmptyMessage,
      );

      expect(mosaicTransferTransaction.fee).to.be.equal(1.25 * 1e6);
    });
  });

  describe("should calculate mosaic fee correctly", () => {
    it("visibility 3, supply 9000000, quantity 150 is 0.05 XEM fee", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        EmptyMessage,
      );

      expect(mosaicTransferTransaction.fee).to.be.equal(0.05 * 1e6);
    });

    it("visibility 0, supply 10_000 the fee is 0.05 no matter the quantity", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(0, 10000), 150000)],
        EmptyMessage,
      );

      expect(mosaicTransferTransaction.fee).to.be.equal(0.05 * 1e6);
    });

    it("visibility 3, supply 9_000_000 the fee is 0.15 xem", () => {
      const mosaic = AssetTransferable.createRelative(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150);
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [mosaic],
        EmptyMessage,
      );

      expect(mosaicTransferTransaction.fee).to.be.equal(0.15 * 1e6);
    });
  });

  it("should calculate mosaic and message fee correctly", () => {

    const mosaicTransferTransaction = TransferTransaction.createWithAssets(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
      PlainMessage.create("message"),
    );

    expect(mosaicTransferTransaction.fee).to.be.equal(100000);
  });

  it("should throw error when recipient address and encrypted message recipient address don't match", () => {
    const account = Account.createWithPrivateKey(privateKey);
    const encryptedMessage = account.encryptMessage("test transaction", PublicAccount.createWithPublicKey(account.publicKey));
    expect(() => {
      TransferTransaction.create(TimeWindow.createWithDeadline(), recipientPublicAccount.address, new XEM(2), encryptedMessage);
    }).to.throw(Error, "Recipient address and recipientPublicAccount don't match");
  });

  it("should throw error when recipient address and encrypted message recipient address don't match in mosaic transfer", () => {
    const account = Account.createWithPrivateKey(privateKey);
    const encryptedMessage = account.encryptMessage("test transaction", PublicAccount.createWithPublicKey(account.publicKey));
    expect(() => {
      TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        recipientPublicAccount.address,
        [new AssetTransferable(new AssetId("multisigns", "mosaic"), new AssetProperties(), 1),
          new AssetTransferable(new AssetId("multisigns", "mosaic2"), new AssetProperties(), 1),
          new AssetTransferable(new AssetId("multisigns", "mosaic3"), new AssetProperties(), 1)],
        encryptedMessage,
      );
    }).to.throw(Error, "Recipient address and recipientPublicAccount don't match");
  });

  it("should decrypt the message", () => {
    // 74657374206d657373616765 == test message
    const transaction = new TransferTransaction(
      new Address("TDM3DOZM5WJ3ZRBPSMYRU6JSWKUCAH5VIPOF4W7K"),
      new XEM(0),
      TimeWindow.createWithDeadline(),
      -1744830463,
      100000,
      PlainMessage.create("test message"),
      "ababe206373af4b8f8ad433232d927cb700" +
      "dd24f18163b2dbcd3b54bf4570cf906b69956a27c0e6288b727611dff3039f7f15466d730614f5487b199891ca504",
      undefined,
      PublicAccount.createWithPublicKey("0414fe7647ec008e533aac98a4bf1c5fbf1d236c75b81fdadf1f5d1042fdd2ff"),
      new TransactionInfo(1052488, 126092, new HashData("4e5fe3b59948cba829d69eaad016d84e9edf74223586e2a602d07acb4452acd9")),
    );
    expect(transaction.message.isPlain()).to.be.true;
    expect(transaction.message.isEncrypted()).to.be.false;
    expect((transaction.message as PlainMessage).plain()).to.be.equal("test message");
  });

  describe("containsMosaics", () => {
    it("should return false when the transaction does not contain mosaics", () => {
      const transferTransaction = TransferTransaction.create(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        new XEM(500000),
        EmptyMessage);

      expect(transferTransaction.containAssets()).to.be.false;
    });

    it("should return true when the transaction contains mosaics", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        PlainMessage.create("message"),
      );

      expect(mosaicTransferTransaction.containAssets()).to.be.true;
    });
  });

  describe("mosaics", () => {
    it("should the same quantity when the transaction mosaics[0] quantity equals expected value multiplier = 1", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        PlainMessage.create("message"),
      );

      expect(mosaicTransferTransaction.assets()[0].quantity).to.be.equal(150);
    });

    it("should take into account quantity when the transaction mosaics[0] amount equals expected value multiplier = 0.5", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        PlainMessage.create("message"),
      );
      const halfMultiplierTransferTransaction = new TransferTransaction(mosaicTransferTransaction.recipient, new XEM(0.5), mosaicTransferTransaction.timeWindow, 2, mosaicTransferTransaction.fee, mosaicTransferTransaction.message, undefined, mosaicTransferTransaction.assets());

      expect(halfMultiplierTransferTransaction.assets()[0].quantity).to.be.equal(75);
    });

    it("should take into account when the transaction mosaics[0] amount equals expected value multiplier = 2", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        PlainMessage.create("message"),
      );
      const doubleMultiplierTransferTransaction = new TransferTransaction(mosaicTransferTransaction.recipient, new XEM(2), mosaicTransferTransaction.timeWindow, 2, mosaicTransferTransaction.fee, mosaicTransferTransaction.message, undefined, mosaicTransferTransaction.assets());

      expect(doubleMultiplierTransferTransaction.assets()[0].quantity).to.be.equal(300);
    });

    it("should throw error when the transaction does not contain mosaics and mosaic function is called", () => {
      const transferTransaction = TransferTransaction.create(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        new XEM(500000),
        EmptyMessage);

      expect(() => {
        transferTransaction.assets();
      }).to.throw(Error);
    });

    it("should throw error when the transaction has mosaics and xem method is called", () => {
      const mosaicTransferTransaction = TransferTransaction.createWithAssets(
        TimeWindow.createWithDeadline(),
        new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
        [new AssetTransferable(new AssetId("test", "test"), new AssetProperties(3, 9000000), 150)],
        PlainMessage.create("message"),
      );

      expect(() => {
        mosaicTransferTransaction.xem();
      }).to.throw(Error);
    });
  })

  it("should return the mosaic identifiers", () => {
    const mosaicId1 = new AssetId("multisigns", "mosaic");
    const mosaicId2 = new AssetId("multisigns", "mosaic2");
    const mosaicId3 = new AssetId("multisigns", "mosaic3");
    const transaction = TransferTransaction.createWithAssets(
      TimeWindow.createWithDeadline(),
      recipientPublicAccount.address,
      [new AssetTransferable(mosaicId1, new AssetProperties(), 1),
        new AssetTransferable(mosaicId2, new AssetProperties(), 1),
        new AssetTransferable(mosaicId3, new AssetProperties(), 1)],
      EmptyMessage,
    );
    expect(transaction.assetsIds()).to.be.deep.equal([mosaicId1, mosaicId2, mosaicId3]);
  });

  it("should throw error when mosaicsIds() is called and it doesn't contain mosaics", () => {
    const transferTransaction = TransferTransaction.create(
      TimeWindow.createWithDeadline(),
      new Address("TCJZJH-AV63RE-2JSKN2-7DFIHZ-RXIHAI-736WXE-OJGA"),
      new XEM(500000),
      EmptyMessage);

    expect(() => {
      transferTransaction.assetsIds();
    }).to.throw(Error);
  });
});

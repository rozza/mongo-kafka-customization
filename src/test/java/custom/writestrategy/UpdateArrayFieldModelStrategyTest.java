/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package custom.writestrategy;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdateArrayFieldModelStrategyTest {

    public static final UpdateArrayFieldModelStrategy WRITE_STRATEGY = new UpdateArrayFieldModelStrategy();

    @Test
    void testUpdateArrayFieldModelStrategy() {
        BsonDocument valueDocument = BsonDocument.parse("{"
            + "'customerId':'ABCDEF', "
            + "'countryCode': 'GB', "
            + "'basic':'BASIC', "
            + "'branch':'01234', "
            + "'suffix':'111', "
            + "'balances': {'availableBalance': 3000}"
            + "}");

        SinkDocument sinkDocument = new SinkDocument(new BsonDocument(), valueDocument);
        WriteModel<BsonDocument> writeModel = WRITE_STRATEGY.createWriteModel(sinkDocument);

        assertTrue(writeModel instanceof UpdateOneModel);
        UpdateOneModel<BsonDocument> updateOneModel = (UpdateOneModel<BsonDocument>) writeModel;


        BsonDocument filterDocument = updateOneModel.getFilter().toBsonDocument();
        assertEquals(BsonDocument.parse("{'$and': ["
                                                + "{'_id': 'ABCDEFGB'}, "
                                                + "{'Accounts': {'$elemMatch': {'$and': [{'Suffix': '111'}, {'Branch': '01234'}]}}}]}"),
                     filterDocument);

        Bson updateBson = updateOneModel.getUpdate();
        assertNotNull(updateBson);
        assertEquals(BsonDocument.parse("{'$set': {'Accounts.$.AvailableBalance': 3000}}") , updateBson.toBsonDocument());
    }
}

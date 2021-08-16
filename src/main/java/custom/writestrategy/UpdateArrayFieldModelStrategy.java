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

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpdateArrayFieldModelStrategy implements WriteModelStrategy {
    private static final String ID = "_id";
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateArrayFieldModelStrategy.class);

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        LOGGER.info("Processing document using UpdateArrayFieldModelStrategy");
        try {
            BsonDocument vd = document
                    .getValueDoc()
                    .orElseThrow(() -> new DataException("Could not build the WriteModel,the value document was missing unexpectedly"));

            LOGGER.info("Converting: {} into a writemodel", vd.toJson());

            BsonString customerId = new BsonString(
                    vd.getString("customerId").getValue() + vd.getString("countryCode").getValue());
            // {"_id":"customerId","accounts": { "$elemMatch": { "suffix": vd.get("suffix"),
            // "branch": vd.get("branch") } }}

            Bson filterBson = Filters.and(Filters.eq(ID, customerId),
                                          Filters.elemMatch("Accounts",
                                                            (Filters.and(Filters.eq("Suffix", vd.get("suffix")),
                                                                         Filters.eq("Branch", vd.get("branch"))))));

            /*
             * BsonDocument filterBson = new Filters(and(
             * eq(ID,customerId),elemMatch("Accounts", new Filters(and(eq("Suffix",
             * vd.get("suffix")),eq("Branch", vd.get("branch"))))))); Bson updateBson =
             * Updates.set("Accounts.$.AvailableBalance",
             * vd.get("balances.availableBalance"));
             */

            Bson updateBson = Updates.set("Accounts.$.AvailableBalance",
                                          vd.getDocument("balances", new BsonDocument()).get("availableBalance"));
            // .append("accounts.$.<otherFields", vd.get("kafkaFieldName"))
            // .append("updatedBy","Kafka")

            LOGGER.info("Update Model filter: {}", filterBson.toBsonDocument().toJson());
            LOGGER.info("Update Model update: {}", updateBson.toBsonDocument().toJson());
            return new UpdateOneModel<>(filterBson, updateBson);
        } catch (Exception e) {
            LOGGER.error("Failed to create the write model because: {}", e.getMessage());
            // return null; // Do this if you want to ignore the SinkDocument and not create a record (not recommended unless there are expected to be some messages to ignore)
            throw new DataException("Could not build the WriteModel: " + e.getMessage());
        }
    }
}

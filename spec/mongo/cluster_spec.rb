require 'spec_helper'

describe Mongo::Cluster do

  describe '#==' do

    let(:preference) do
      Mongo::ServerPreference.get
    end

    let(:cluster) do
      described_class.new([ '127.0.0.1:27017' ], preference)
    end

    context 'when the other is a cluster' do

      context 'when the addresses are the same' do

        context 'when the options are the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], preference)
          end

          it 'returns true' do
            expect(cluster).to eq(other)
          end
        end

        context 'when the options are not the same' do

          let(:other) do
            described_class.new([ '127.0.0.1:27017' ], preference, :replica_set => 'test')
          end

          it 'returns false' do
            expect(cluster).to_not eq(other)
          end
        end
      end

      context 'when the addresses are not the same' do

        let(:other) do
          described_class.new([ '127.0.0.1:27018' ], preference)
        end

        it 'returns false' do
          expect(cluster).to_not eq(other)
        end
      end
    end

    context 'when the other is not a cluster' do

      it 'returns false' do
        expect(cluster).to_not eq('test')
      end
    end
  end

  pending '#inspect'
end

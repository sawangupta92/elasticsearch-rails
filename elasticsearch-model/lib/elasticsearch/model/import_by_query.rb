module Elasticsearch
  module Model

    # Provides support for easily and efficiently importing large amounts of
    # records from the including class into the index.
    #
    # @see ClassMethods#query_import
    #
    module ImportByQuery

      # When included in a model, adds the importing methods.
      #
      # @example Import all records from the `Article` model
      #
      #     Article.query_import
      #
      # @see #query_import
      #
      def self.included(base)
        base.__send__ :extend, ClassMethods

        adapter = Adapter.from_class(base)
        base.__send__ :include, adapter.importing_mixin
        base.__send__ :extend,  adapter.importing_mixin
      end

      module ClassMethods

        # Import all model records into the index
        #
        # The method will pick up correct strategy based on the `Importing` module
        # defined in the corresponding adapter.
        #
        # @param options [Hash] Options passed to the underlying `__find_in_batches`method
        # @param block  [Proc] Optional block to evaluate for each batch
        #
        # @yield [Hash] Gives the Hash with the Elasticsearch response to the block
        #
        # @return [Fixnum] Number of errors encountered during importing
        #
        # @example Import all records into the index
        #
        #     Article.query_import
        #
        # @example Set the batch size to 100
        #
        #     Article.query_import batch_size: 100
        #
        # @example Process the response from Elasticsearch
        #
        #     Article.query_import do |response|
        #       puts "Got " + response['items'].select { |i| i['index']['error'] }.size.to_s + " errors"
        #     end
        #
        # @example Delete and create the index with appropriate settings and mappings
        #
        #    Article.query_import force: true
        #
        # @example Refresh the index after importing all batches
        #
        #    Article.query_import refresh: true
        #
        # @example Import the records into a different index/type than the default one
        #
        #    Article.query_import index: 'my-new-index', type: 'my-other-type'
        #
        # @example Pass an ActiveRecord scope to limit the imported records
        #
        #    Article.query_import scope: 'published'
        #
        # @example Pass an ActiveRecord query to limit the imported records
        #
        #    Article.query_import query: -> { where(author_id: author_id) }
        #
        # @example Transform records during the query_import with a lambda
        #
        #    transform = lambda do |a|
        #      {index: {_id: a.id, _parent: a.author_id, data: a.__elasticsearch__.as_indexed_json}}
        #    end
        #
        #    Article.query_import transform: transform
        #
        # @example Update the batch before yielding it
        #
        #     class Article
        #       # ...
        #       def self.enrich(batch)
        #         batch.each do |item|
        #           item.metadata = MyAPI.get_metadata(item.id)
        #         end
        #         batch
        #       end
        #     end
        #
        #    Article.query_import preprocess: :enrich
        #
        # @example Return an array of error elements instead of the number of errors, eg.
        #          to try importing these records again
        #
        #    Article.query_import return: 'errors'
        #
        def query_import(options={}, &block)
          errors       = []
          refresh      = options.delete(:refresh)   || false
          target_index = options.delete(:index)     || index_name
          target_type  = options.delete(:type)      || document_type
          transform    = options.delete(:transform) || __transform
          return_value = options.delete(:return)    || 'count'

          unless transform.respond_to?(:call)
            raise ArgumentError,
                  "Pass an object responding to `call` as the :transform option, #{transform.class} given"
          end

          if options.delete(:force)
            self.create_index! force: true, index: target_index
          elsif !self.index_exists? index: target_index
            raise ArgumentError,
                  "#{target_index} does not exist to be imported into. Use create_index! or the :force option to create it."
          end

          # __find_in_batches(options) do |batch|
          response = client.update_by_query \
                        index:   target_index,
                        type:    target_type,
                        body:    options[:body]

            # yield response if block_given?

          # end

          self.refresh_index! index: target_index if refresh

          case return_value
            when 'errors'
              errors
            else
              errors.size
          end
          response
        end

        # def __batch_to_bulk(batch, transform)
        #   batch.map { |model| transform.call(model) }
        # end
      end

    end

  end
end

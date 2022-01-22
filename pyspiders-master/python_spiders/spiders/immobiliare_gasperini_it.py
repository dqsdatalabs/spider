import scrapy
from ..loaders import ListingLoader


class MySpider(scrapy.Spider):
    name = 'immobiliare_gasperini_it'
    allowed_domains = ['immobiliare-gasperini.it']
    start_urls = [
        'https://www.immobiliare-gasperini.it/annunci/?filter-property-title=&filter-contract=RENT&filter-property-type=49']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        lnks = response.css('.entry-title a').xpath('@href').extract()
        links = list(dict.fromkeys(lnks))
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        if "Vendita" in response.css('#property-section-detail > ul > li:nth-child(6)::text').get():
            return

        item_loader.add_value('external_link', response.url)
        title = response.css('.property-title::text').extract()[0]
        item_loader.add_value('title', title)
        address = response.css(
            '.pull-left .property-row-address::text').extract()[1].strip()
        item_loader.add_value('address', address)
        d = address
        item_loader.add_value('city', d.split(',')[-1].strip())
        zip = d.split(',')
        if len(zip) > 2:
            item_loader.add_value('zipcode', zip[-2].strip())
        description = ''.join(response.css(
            '#property-section-description ::text').extract()[3:]).strip()
        item_loader.add_value('description', description)
        rent = response.css(
            '.property-box-price::text').extract()[0][:-2].strip()
        item_loader.add_value('rent_string', rent)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', response.css(
            '.agent-small-title a::text').extract()[0])
        item_loader.add_value('landlord_phone', response.css(
            '.agent-small-phone::text').extract()[1].strip())
        item_loader.add_value('landlord_email', response.css(
            '.agent-small-email a::text').extract()[0])
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('property_type', 'apartment')
        imgs = response.css('img').xpath('@src').extract()
        images = [i for i in imgs if '750x450' in i]
        item_loader.add_value('images', images)
        if 'trilocale' in title.lower():
            item_loader.add_value('room_count', 3)
        if 'bilocale' in title.lower():
            item_loader.add_value('room_count', 2)
        if 'monolocale' in title.lower():
            item_loader.add_value('room_count', 1)

        details = response.css('#property-section-detail ::text').extract()
        stripped_details = [i.strip() for i in details]
        if'Superficie:' in stripped_details:
            i = stripped_details.index('Superficie:')
            item = stripped_details[i+1]
            item_loader.add_value('square_meters', int(
                item[:item.index('mq')].strip()))
        if 'Stanze:' in stripped_details:
            i = stripped_details.index('Stanze:')
            item = stripped_details[i + 1]
            item_loader.add_value('room_count', int(item))
        if 'Bagni:' in stripped_details:
            i = stripped_details.index('Bagni:')
            item = stripped_details[i + 1]
            item_loader.add_value('bathroom_count', int(item))
        if 'Riferimento:' in stripped_details:
            i = stripped_details.index('Riferimento:')
            item = stripped_details[i + 1]
            item_loader.add_value('external_id', item)

        services = response.css('.columns-gap li').xpath('@class').extract()
        fur = services[0]
        elev = services[2]
        balcony = services[3]
        if fur == 'yes':
            item_loader.add_value('furnished', True)
        else:
            item_loader.add_value('furnished', False)
        if elev == 'yes':
            item_loader.add_value('elevator', True)
        else:
            item_loader.add_value('elevator', False)
        if balcony == 'yes':
            item_loader.add_value('balcony', True)
        else:
            item_loader.add_value('balcony', False)

        lat = response.css(
            'div.property-box.property-box-grid.property-box-wrapper::attr(data-latitude)').get()
        lng = response.css(
            'div.property-box.property-box-grid.property-box-wrapper::attr(data-longitude)').get()

        item_loader.add_value('latitude', lat)
        item_loader.add_value('longitude', lng)

        if 'vendita' not in response.url:
            yield item_loader.load_item()
import re

import scrapy
from scrapy import Request

from ..helper import remove_unicode_char, extract_number_only
from ..loaders import ListingLoader


class WebsiteDomainSpider(scrapy.Spider):
    name = 'thefairwaygroup_ca'
    allowed_domains = ['thefairwaygroup.ca']
    start_urls = ['https://thefairwaygroup.ca/residential/']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'development'

    def parse(self, response):
        for start_url in self.start_urls:
            yield Request(url=start_url,
                          callback=self.parse_building,
                          )

    def parse_building(self, response):
        buildings = response.css('.elementor-size-xl a::attr(href)').extract()
        for building in buildings:
            yield Request(url=building,
                          callback=self.parse_building_rentals)

    def parse_building_rentals(self, response):
        rentals = response.css('.item-title a::attr(href)').extract()
        for rental in rentals:
            yield Request(url=rental,
                          callback=self.parse_area_pages)

    def parse_area_pages(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        external_source = self.external_source
        title = response.css('h1::text').extract_first()
        containers = response.css('.vc_tta-panel')
        description = ''
        ammenities = ''
        for panel in containers:
            header = panel.css('div.vc_tta-panel-heading > h4 > a > span > b::text').extract_first()
            if header == "Description":
                description = remove_unicode_char((((' '.join(panel.css('.wpb_wrapper ::text').extract()).replace('\n','')).replace('\t','')).replace('\r','')))
                description = description.split('CALL US ABOUT THIS PROPERTY:')
                description = description[0]
            if header == "Ammenities":
                ammenities = (" ".join(panel.css('.wpb_wrapper ::text').extract())).lower()
            if header == 'Units':
                floor_plan_images = panel.css('.vc_tta-panel-body noscript img::attr(src)').extract()

        address = response.css('.item-address::text').extract_first()
        city = response.css('.detail-city span::text').extract_first()
        zipcode = response.css('.detail-zip span::text').extract_first()
        lat_script = response.css("script:contains(houzez_single_property_map)").get()
        lat_script = re.findall('"lat":"(-?\d+.\d+)","lng":"(-?\d+.\d+)',lat_script)
        if 'studio' in title.lower():
            property_type = 'studio'
        else:
            property_type = 'apartment'
        room_details = response.css('.list-unstyled')
        room_count = 1
        bathroom_count = 1
        for room in room_details:
            header = room.css('li:nth-child(2)::text').extract_first()
            if header:
                if 'Bedroom' in header:
                    room_count_text = extract_number_only(room.css('li:nth-child(1) strong::text').extract_first())
                    room_count = room_count_text
                    if room_count == 0:
                        room_count = 1
                if 'Bathroom' in header:
                    bathroom_count_text = (room.css('li:nth-child(1) strong::text').extract_first())
                    if '1/2' in bathroom_count_text:
                        bathroom_count = int(extract_number_only(bathroom_count_text)) + 1
                    else:
                        bathroom_count = extract_number_only(bathroom_count_text)
                    if bathroom_count == 0:
                        bathroom_count = 1

        images = response.css('#lightbox-slider-js div img::attr(data-src)').extract()
        external_images_count = len(images)
        rent = int(extract_number_only(extract_number_only(response.css('.item-price::text').extract_first())))
        currency = 'CAD'

        if ('parking' in description) or ('parking' in ammenities):
            parking = True
        else:
            parking = False

        if ('elevator' in description) or ('elevator' in ammenities):
            elevator = True
        else:
            elevator = False


        if ('terrace' in description) or ('terrace' in ammenities):
            terrace = True
        else:
            terrace = False

        if ('swim' in description) or ('swim' in ammenities) or  ('pool' in description) or ('pool' in ammenities):
            swimming_pool = True
        else:
            swimming_pool = False

        if ('laundry' in description) or ('laundry' in ammenities):
            washing_machine = True
        else:
            washing_machine = False

        if (' dishwasher' in description) or (' dishwasher' in ammenities):
            dishwasher = True
        else:
            dishwasher = False


        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_source', external_source)
        item_loader.add_value('title', title)
        item_loader.add_value('description', description)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_value('address', address)

        item_loader.add_value('latitude', lat_script[0][0])
        item_loader.add_value('longitude', lat_script[0][1])
        item_loader.add_value('square_meters', '1')
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('room_count', room_count)
        item_loader.add_value('bathroom_count', bathroom_count)
        item_loader.add_value("images", images)
        item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("external_images_count", external_images_count)
        item_loader.add_value("rent", rent)
        item_loader.add_value("currency", currency)
        item_loader.add_value("parking", parking)
        item_loader.add_value("elevator", elevator)
        item_loader.add_value("terrace", terrace)
        item_loader.add_value("swimming_pool", swimming_pool)
        item_loader.add_value("washing_machine", washing_machine)
        item_loader.add_value("dishwasher", dishwasher)
        item_loader.add_value("landlord_name", 'fairway group')
        item_loader.add_value("landlord_email", 'fairway@groupefairway.com')
        item_loader.add_value("landlord_phone", '(514) 342 2791')

        yield item_loader.load_item()

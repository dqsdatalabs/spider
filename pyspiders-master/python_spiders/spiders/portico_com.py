# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only
from scrapy import Selector
import js2xml
import lxml


class PorticoSpider(scrapy.Spider):
    name = "portico_com"
    allowed_domains = ["www.portico.com"]
    start_urls = (
        'http://www.www.portico.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ['https://www.portico.com/london/rent/properties/']
        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="search_results_panel"]/div[contains(@id,"pp_load") or contains(@class,"property_module")]')
        for listing in listings:
            property_url = response.urljoin(listing.xpath('.//a[contains(@href,"rent/")]/@href').extract_first())
            yield scrapy.Request(url=property_url, callback=self.get_property_details, meta={'request_url': property_url})

    def get_property_details(self, response):
        # checks for already let properties
        if response.xpath('.//div[contains(text(),"This rental property has been let")]').extract_first():
            return

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Portico_PySpider_{}_{}".format(self.country, self.locale))
        room_count = response.xpath('//img[contains(@src,"/bed")]/parent::*/following-sibling::text()[contains(.,"Studio")]').get()
        if room_count:
            item_loader.add_value("room_count", "1")
        else:
            item_loader.add_value("room_count", extract_number_only(response.xpath('.//img[contains(@src,"/bed")]/parent::*/following-sibling::text()')))
        item_loader.add_value("bathroom_count", extract_number_only(response.xpath('.//img[contains(@src,"/bath")]/parent::*/following-sibling::text()')))
        item_loader.add_xpath('description', './/div[contains(text(),"Property Description")]/parent::*/parent::*/following-sibling::div/div/text()')
        item_loader.add_xpath('rent_string', './/h2/span[contains(@class,"teninfo")]/text()')
        item_loader.add_xpath('title', './/meta[contains(@id,"OGTitle")]/@content')

        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage', 'student property']
        studio_types = ["studio"]
        
        # property_type
        studio = response.xpath('//img[contains(@src,"/bed")]/parent::*/following-sibling::text()[contains(.,"Studio")]').get()
        if studio:
            item_loader.add_value("property_type", "studio")
        elif any(i in item_loader.get_output_value('description').lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in item_loader.get_output_value('description').lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in item_loader.get_output_value('description').lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        item_loader.add_xpath('address', './/h1')

        if item_loader.get_output_value('address'):
            if any(ch.isdigit() for ch in item_loader.get_output_value('address').split(', ')[-1]):
                item_loader.add_value('zipcode', item_loader.get_output_value('address').split(', ')[-1])
                item_loader.add_value('city', item_loader.get_output_value('address').split(', ')[-2])

        floor_plan_images = response.xpath('.//a[contains(@id,"OpenFloorplan")]/@href').extract()
        for image in floor_plan_images:
            item_loader.add_value('floor_plan_images', response.urljoin(image))
            
        images = response.xpath('.//ul[@id="property_images"]/li/img/@src').extract()
        for image in images:
            item_loader.add_value('images', response.urljoin(image))

        javascript = response.xpath('.//script[contains(text(), "LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)

            item_loader.add_value('latitude', xml_selector.xpath('.//var[@name="lat"]/number/@value').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//var[@name="lon"]/number/@value').extract_first())

        features = ' '.join(response.xpath('.//img[contains(@src,"/bath")]/parent::*/parent::*/following-sibling::div/text()').extract())

        if ' furnished' in features.lower() and 'unfurnished' not in features.lower():
            item_loader.add_value('furnished', True)
        elif 'unfurnished' in features.lower() and ' furnished' not in features.lower():
            item_loader.add_value('furnished', False)

        # https://www.portico.com/highbury/rent/properties/4-bedroom/7fPoT71pO.html
        if 'parking' in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        # https://www.portico.com/hammersmith/rent/properties/2-bedroom/5gjcdtyX4.html
        if 'elevator' in features.lower() or 'lift' in features.lower():
            item_loader.add_value('elevator', True)
        
        # https://www.portico.com/hammersmith/rent/properties/2-bedroom/5gjcdtyX4.html
        if 'balcony' in features.lower():
            item_loader.add_value('balcony', True)

        # https://www.portico.com/islington/rent/properties/3-bedroom/5bsbHixTK.html
        if 'terrace' in features.lower():
            item_loader.add_value('terrace', True)

        if 'swimming pool' in features.lower():
            item_loader.add_value('swimming_pool', True)

        if 'washing machine' in features.lower():
            item_loader.add_value('washing_machine', True)

        if 'dishwasher' in features.lower():
            item_loader.add_value('dishwasher', True)

        epc = response.xpath('.//a[contains(@id,"OpenEPC")]/@href').extract_first()
        epc_class = None
        if epc and epc.split('_')[1].isnumeric():
            rating_value = epc.split('_')[1]
            rating_value = int(rating_value)
            if rating_value >= 92 and rating_value <= 100:
                epc_class = 'A'
            elif rating_value >= 81 and rating_value <= 91:
                epc_class = 'B'
            elif rating_value >= 69 and rating_value <= 80:
                epc_class = 'C'
            elif rating_value >= 55 and rating_value <= 68:
                epc_class = 'D'
            elif rating_value >= 39 and rating_value <= 54:
                epc_class = 'E'
            elif rating_value >= 21 and rating_value <= 38:
                epc_class = 'F'
            elif rating_value >= 1 and rating_value <= 20:
                epc_class = 'G'
        if epc_class:
            item_loader.add_value('energy_label', epc_class)

        item_loader.add_value('landlord_name', 'Portico')
        item_loader.add_value('landlord_phone', extract_number_only(response.xpath('.//a[contains(@id,"OfficeLink")]/following-sibling::text()').extract_first().replace(' ','')))
        item_loader.add_value('landlord_email', 'acton@portico.com')

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()

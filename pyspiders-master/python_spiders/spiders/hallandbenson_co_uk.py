# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from datetime import datetime
import re
import js2xml
import lxml.etree 
import scrapy
from scrapy import Selector
from datetime import datetime,date,timedelta
from ..helper import extract_number_only, remove_unicode_char, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader


class HallandbensonCoUkSpider(scrapy.Spider):
    name = 'hallandbenson_co_uk'
    allowed_domains = ['www.hallandbenson.co.uk']
    start_urls = ['https://www.hallandbenson.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    external_source = "Hallandbenson_PySpider_united_kingdom_en"
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = ["https://www.hallandbenson.co.uk/properties/lettings"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath("//div[@class='photos property-status-container']/a")
        for property_item in listings:
            external_id = property_item.xpath(".//@href").extract_first().split('/')[2]
            url = "https://www.hallandbenson.co.uk" + property_item.xpath(".//@href").extract_first()

            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'request_url': url,
                      'external_id': external_id})

        next_page = response.xpath('.//a[@rel="next"]/@href').extract_first()
        if next_page:
            next_page_url = "https://www.hallandbenson.co.uk" + next_page
            yield response.follow(
                url=next_page_url,
                callback=self.parse)

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        item_loader.add_xpath("title", ".//div[@id = 'property-show']/h3/text()")
        item_loader.add_xpath('images', ".//div[@class='slide-content wide-image']//@href")

        floor_plan_images = response.xpath(".//div[@id = 'floorplan-slideshow-container']//@href").extract()
        if len(floor_plan_images) > 0:
            floor_plan_images = ["https:" + img_i for img_i in floor_plan_images]
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        rent= response.xpath("//div[@class='price']/text()").extract_first()
        if rent:
            item_loader.add_value("rent",rent.split("£")[1].split("pcm")[0])
        item_loader.add_value("currency","GBP")
        

        if response.xpath(".//li[@class='bedrooms']"):
            item_loader.add_xpath('room_count', "//li[@class='bedrooms']/span/text()")
        else:
            return
        if response.xpath(".//li[@class='bathrooms']"):
            item_loader.add_xpath('bathroom_count', "//li[@class='bathrooms']/span/text()")

        item_loader.add_xpath('address', ".//div[@id = 'property-show']/h3/text()")
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(", ")[-1])
        item_loader.add_value('city', item_loader.get_output_value('address').split(", ")[-2])

        # I've not included application information in the description
        description = " ".join(response.xpath(".//div[@id='description-content']/div/p/text()").extract()[:-1])
        if description:
            item_loader.add_value('description', remove_white_spaces(description))
        else:
            description = response.xpath("//div[@id='description-content']/div/text()").get()
            item_loader.add_value("description",description)

        javascript = response.xpath('(.//*[contains(text(),"lng")])[1]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').get()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').get()        
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        item_loader.add_value('landlord_name', 'Hall & Benson')
        item_loader.add_value('landlord_phone', "01332 555 949")
        
        features = "".join(response.xpath("(//ul[@class='property-features'])[1]/li/text()").extract())
        availability = remove_white_spaces(features) + remove_white_spaces(description)

        # EPC check
        epc = re.search(r"EPC BAND (\w{1})", features.lower(), re.IGNORECASE)
        if epc:
            epc = epc.group(1)
            item_loader.add_value('energy_label', epc.upper())

        # available_date
        if "available from" in availability.lower():
            check = re.search(r'available from (\d{2}(?:\w{2})? \w+ \d{4})', availability.lower(), re.IGNORECASE)
            if check:
                available_date = check.group(1)
                available_date = available_date.lower().replace("rd", "").replace("nd", "").replace("st", "").replace("th","")
                available_date = datetime.strptime(available_date, "%d %B %Y").strftime('%Y-%m-%d')
                item_loader.add_value('available_date', available_date)

        if "available after" in availability.lower():
            check = re.search(r'available after (\d{2}(?:\w{2})? \w+ \d{4})',availability.lower(),re.IGNORECASE)            
            if check:
                available_date = check.group(1)
                available_date = available_date.lower().replace("rd", "").replace("nd", "").replace("st", "").replace("th","")
                available_date = datetime.strptime(available_date, "%d %B %Y")
                available_date = available_date + timedelta(days=1)
                available_date = datetime.strftime(available_date, '%Y-%m-%d')
                item_loader.add_value('available_date', available_date)

        # two properties get skipped due to inability to get property type
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['chalet', 'bungalow', 'maison', 'house', 'home', ' villa ',
                       'holiday complex', 'cottage', 'garden','semi-detached', "barn conversion"]
        studio_types = ["studio"]
        if any(i in features.lower() or i in description.lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in features.lower() or i in description.lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in features.lower() or i in description.lower() for i in house_types):
            item_loader.add_value('property_type', 'house')

        # https://www.hallandbenson.co.uk/properties/13411646/lettings
        if "parking" in features.lower() or 'garage' in features.lower():
            item_loader.add_value('parking', True)

        # https://www.hallandbenson.co.uk/properties/13412147/lettings
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.hallandbenson.co.uk/properties/13411877/lettings
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        # https://www.hallandbenson.co.uk/properties/13411877/lettings
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)

        if "swimming" in features.lower() or "pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if " furnished" in features.lower() and "unfurnished" not in features.lower():
            item_loader.add_value('furnished', True)
        
        # https://www.hallandbenson.co.uk/properties/13413326/lettings
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)
        
        # https://www.hallandbenson.co.uk/properties/13435747/lettings
        if "strictly no pets" in features.lower() or "no pets" in features.lower():
            item_loader.add_value('pets_allowed', False)
        
        # https://www.hallandbenson.co.uk/properties/13411826/lettings
        if "pet permitted" in features.lower():
            item_loader.add_value('pets_allowed', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("landlord_email","lettings@hallandbenson.co.uk")
        yield item_loader.load_item()
 
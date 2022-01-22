# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import lxml
import scrapy
import js2xml
import re
from scrapy import Selector
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_number_only, extract_rent_currency, format_date
from geopy.geocoders import Nominatim
from ..user_agents import random_user_agent
from word2number import w2n

class ParkerspropertiesSpider(scrapy.Spider):
    name = 'parkersproperties_co_uk'
    allowed_domains = ['parkersproperties.co.uk']
    start_urls = ['https://www.parkersproperties.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.parkersproperties.co.uk/property?intent=rent&location=&radius=&type=houses&price-per=pcm&bedrooms=&include-sold=rent&sort-by=price-desc&per-page=24',
                'property_type': 'house',
                'param': 'houses'},
            {'url': 'https://www.parkersproperties.co.uk/property?intent=rent&location=&radius=&type=flats_apartments&price-per=pcm&bedrooms=&include-sold=rent&sort-by=price-desc&per-page=24',
                'property_type': 'apartment',
                'param': 'flats_apartments'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'param': url.get('param'),
                                       'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        no_of_properties = response.xpath('.//div[contains(@class,"page-tracker")]//strong/text()').extract_first()
        default_page_size = 24
        pages = int(int(no_of_properties) / default_page_size) + 1
        for page in range(1, pages + 1):
            req_url = f"https://www.parkersproperties.co.uk/property?p={page}&per-page=24&intent=rent&price-per=pcm&type={response.meta.get('param')}&include-sold=rent&sort-by=price-desc"
            yield scrapy.Request(
                url=req_url,
                callback=self.get_property_links,
                meta={'property_type': response.meta.get('property_type')})

    def get_property_links(self, response):
        # listings = response.xpath('.//button[contains(text(),"information")]//parent::a/@href').extract()
        listings = response.xpath('.//div[contains(@class, "property-index")]/div')
        for property_item in listings:
            property_category = property_item.xpath('.//p[@class="property-category"]/text()').extract_first().lower()
            property_url = property_item.xpath('.//button[contains(text(),"information")]//parent::a/@href').extract_first()
            if "let agreed" not in property_category:
                yield scrapy.Request(
                    url=property_url,
                    callback=self.get_property_details,
                    meta={'request_url': property_url,
                          'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url')
        address = remove_white_spaces(response.xpath('.//h2[@class="text-secondary"]/text()').extract_first())
        rent = response.xpath('.//strong[@id="propertyPrice"]/text()').extract_first().split("pm")[0]
        tel = response.xpath('.//a[contains(@href,"tel")]/@href').extract_first().split(":")[-1]
        landlord_name = response.xpath('.//h3[@class="text-secondary"]/text()').extract_first().split("Contact ")[-1]
        features = ' '.join(response.xpath('.//ul[@id="propList"]/li/text()').extract()).lower()

        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("/")[-1])
        item_loader.add_xpath('title', './/h2[@class="text-tertiary"]/text()')
        item_loader.add_value('address', address)
        item_loader.add_value('rent_string', rent)
        
        description = "".join(response.xpath("//div[contains(@class,'property-description')]/text()").getall())
        if description:
            item_loader.add_value('description', description.strip())
        
        if "studio" in description.lower():
            item_loader.add_value('property_type', "studio")
        else:
            item_loader.add_value('property_type', response.meta.get('property_type'))
            
        
        item_loader.add_xpath('images', './/img[contains(@alt,"Gallery Image")]/@src')
        item_loader.add_value('room_count', extract_number_only(item_loader.get_output_value('title')))
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_xpath('landlord_email', './/a[contains(@href,"mailto")]/p/text()')
        item_loader.add_value('landlord_phone', tel)
        item_loader.add_xpath('floor_plan_images', './/button[contains(text(),"Floor")]/@data-src')

        javascript = response.xpath('.//script[contains(text(),"setView")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="setView"]/../../../../arguments/array/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="setView"]/../../../../arguments/array/number/@value').extract()[1]

            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-1].split(" ")[-2])
        item_loader.add_value('zipcode', item_loader.get_output_value('address').split(',')[-1].split(" ")[-1])

        # ex https://www.parkersproperties.co.uk/property/to-rent/255461 in key points features
        if 'parking' in item_loader.get_output_value('description').lower() or 'parking' in features or 'garage' in features:
            item_loader.add_value('parking', True)

        date = response.xpath('.//ul[@id="propList"]/li[contains(text(),"Available")]/text()').extract_first()
        if date:
            regex_pattern = r"Available (?P<date>(\w+)) (?P<month>(\w+)) (?P<year>(\d+))"
            regex = re.compile(regex_pattern)
            match = regex.search(date)
            months = ['January','February','March','April','May','June','July','August','September','October','November','December']
            if match:
                available_date = f"{extract_number_only(match['date'])}/{months.index(match['month']) + 1}/{match['year']}"
                item_loader.add_value('available_date', format_date(available_date))
        # ex https://www.parkersproperties.co.uk/property/to-rent/389869
        if " furnished " in item_loader.get_output_value('description').lower() or ' furnished' in features:
            item_loader.add_value('furnished', True)
        # https://www.parkersproperties.co.uk/property/to-rent/386719
        elif " unfurnished " in item_loader.get_output_value('description').lower() or ' unfurnished' in features:
            item_loader.add_value('furnished', False)

        # https://www.parkersproperties.co.uk/property/to-rent/252084
        # PLease look into this. False value doesnt seem to be accepted by the Json
        if "no pets" in item_loader.get_output_value('description').lower() or 'no pets' in features:
            item_loader.add_value('pets_allowed', False)
        # https://www.parkersproperties.co.uk/property/to-rent/389869
        elif "pets accepted" in item_loader.get_output_value('description').lower() or 'pets accepted' in features:
            item_loader.add_value('pets_allowed', True)

        deposit = response.xpath("//li[contains(.,'Deposit')]/text()").get()
        if deposit:
            rent_week = response.xpath('.//strong[@id="propertyPrice"]/text()').extract_first()
            rent = rent.replace("£","").strip()
            if "month" in deposit.lower():
                deposit = deposit.lower().split("month")[0].strip().split(" ")[-1]
                try:
                    deposit = int(float(rent))*w2n.word_to_num(deposit)
                    item_loader.add_value("deposit", deposit)
                except: pass
            elif "week" in deposit.lower() and rent_week and "pw" in rent_week:
                deposit = deposit.lower().split("week")[0].strip().split(" ")[-1].replace(",","")
                rent = rent_week.split("pw")[0].strip().split("£")[-1].replace(",","")
                item_loader.add_value("deposit", int(float(rent))*int(deposit))
                
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Parkersproperties_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
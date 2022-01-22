# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from math import ceil
from ..loaders import ListingLoader
from ..helper import extract_number_only, extract_rent_currency
from word2number import w2n


class GartonJonesSpider(scrapy.Spider):
    name = "gartonjones_com"
    allowed_domains = ['gartonjones.com']
    start_urls = ['https://www.gartonjones.com']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_url = ["https://www.gartonjones.com/properties/?search=1&status=let"]
        for url in start_url:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//li[@data-type="link"]/..')
        for property_item in listings:
            yield scrapy.Request(url=property_item.xpath('.//li/@data-url').extract_first(),
                                 callback=self.get_property_details,
                                 meta={'request_url': property_item.xpath('.//li/@data-url').extract_first(),
                                       'square_meters': property_item.xpath('.//p[contains(text(),"sqft")]/text()').extract_first(),
                                       'room_count': property_item.xpath('.//p[contains(text(),"Bedroom")]/text()').extract_first()})
        
        next_page_url = response.xpath('//a[contains(text(),"Next Page")]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse,
                                 meta={'request_url': next_page_url})

    def get_property_details(self, response):
        
        address = response.xpath('.//div[@class="letorsale"]/h1/text()').extract_first()
        description = response.xpath('.//p[@class="default-fonts"]/text()').extract_first()
        utilities = response.xpath('.//div[@class="feat row"]//div/text()').extract()

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        if len(re.findall(r"-\d{8}-", response.meta.get('request_url'))) > 0:
            item_loader.add_value('external_id', re.findall(r"-\d{8}-", response.meta.get('request_url'))[0].strip("-"))
        else:
            external_id = response.url.split("-")[-1].split("/")[0]
            item_loader.add_value("external_id", external_id)

        item_loader.add_value('title', address)
        if response.meta.get('square_meters') and int(extract_number_only(response.meta.get('square_meters'))) != 0:
            item_loader.add_value('square_meters', str(int(float(extract_number_only(response.meta.get('square_meters')))*0.092903)))
        else:
            left_feat_columns = response.xpath('//div[contains(@class, "leftfeat columns")]/text()[contains(.,"sq") or contains(.,"Sq")]').get()
            if left_feat_columns:
                left_feat_columns = left_feat_columns.strip().split(" ")[0]
                item_loader.add_value('square_meters', str(int(float(left_feat_columns)*0.092903)))
        item_loader.add_value('room_count', extract_number_only(response.meta.get('room_count')))
        
        address = response.xpath("//h1//text()").get()
        if address:
            city = address.replace(", SW11","").replace(", SE1","").replace(", SW1W","").replace(", W2","").replace(",  SW8","").replace(", SW8","").replace(", SW6","").replace(",SW11","")
            city = city.split(",")[-1].strip()
            zipcode = address.strip().split(" ")[-1]
            if "," in zipcode: zipcode = zipcode.split(",")[-1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            if not ("Knightsbridge" in zipcode or "London" in zipcode or "Road" in zipcode):
                if "," in zipcode:
                    zipcode = zipcode.split(",")[-1]
            
            if not zipcode.split(" ")[0].isalpha():
                item_loader.add_value("zipcode", zipcode)

        rent = "".join(response.xpath("//div[contains(@class,'letorsale')]//h2/text()").getall())
        if rent:
            rent = rent.split("£")[1].split("pw")[0].replace(",","").strip()
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "GBP")
        item_loader.add_value('description', description)
        item_loader.add_value('landlord_phone', '0207 730 5007')
        item_loader.add_xpath('landlord_phone', '//div[@id="call-the-office"]/span/text()')

        landlord_name = response.xpath('//div[@class="office small-12 medium-2 large-2 columns text-center"]/a/text()').get()
        if landlord_name:
            item_loader.add_value('landlord_name', landlord_name)
        else:
            item_loader.add_value("landlord_name", "GARTON JONES")

        item_loader.add_xpath('images', '//div[@class="swiper-wrapper"]/div/@data-background-url')

        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis', 'cottage']
        studio_types = ["studio"]
        
        # property_type
        if any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in studio_types):
            item_loader.add_value('property_type', 'studio')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in apartment_types):
            item_loader.add_value('property_type', 'apartment')
        elif any(i in ' '.join(item_loader.get_output_value('description').split('.')[:3]).lower() for i in house_types):
            item_loader.add_value('property_type', 'house')
        else:
            return

        lat_long_map = response.xpath('//a[@id="vmap"]/@onclick').extract_first()
        if lat_long_map:
            lat_long = lat_long_map.split('vmap(')[-1].split(');')[0]
            item_loader.add_value('latitude', lat_long.split(',')[0])
            item_loader.add_value('longitude', lat_long.split(',')[-1])
        
        bathroom_count = response.xpath("//div[@class='feat row']//div/text()[contains(.,'Bathroom') or contains(.,'bathroom')]").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split("bathroom")[0].replace("modern","").replace("luxury","").replace("en-suite","").strip()
            if "," in bathroom_count:
                bathroom_count = bathroom_count.strip().split(" ")[-1]
            elif "&" in bathroom_count:
                bathroom_count = bathroom_count.strip().split(" ")[-1]
            else:
                bathroom_count = bathroom_count.strip().split(" ")[0]
            if bathroom_count.isdigit():
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                try:
                    item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count))
                except : pass

        for utility in utilities:            
            if "parking" in utility.lower() or "parking" in description.lower():
                item_loader.add_value('parking', True)
            
            elif "terrace" in utility.lower() or "terrace" in description.lower():
                item_loader.add_value('terrace', True)

            elif "swimming pool" in utility.lower() or "swimming pool" in description.lower():
                item_loader.add_value('swimming_pool', True)
            
            elif "elevator" in utility.lower() or "elevator" in description.lower():
                item_loader.add_value('elevator', True)
            
            elif "balcony" in utility.lower() or "balcony" in description.lower():
                item_loader.add_value('balcony', True)

            elif "unfurnished" in utility.lower() or "unfurnished" in description.lower():
                item_loader.add_value('furnished', False)
            elif "furnished" in utility.lower() or "furnished" in description.lower():
                item_loader.add_value('furnished', True)

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Gartonjones_PySpider_{}_{}".format(self.country, self.locale))
        status = response.xpath("//div[contains(@class,'mobileagreed')]//text()[contains(.,'LET AGREED')]").get()
        if status:
            return
        else:
            yield item_loader.load_item()

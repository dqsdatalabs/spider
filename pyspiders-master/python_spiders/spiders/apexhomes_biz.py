# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from scrapy import Selector
from ..loaders import ListingLoader
from ..helper import extract_number_only,format_date
import lxml
import js2xml
import re

class ApexhomesBizSpider(scrapy.Spider):
    name = "apexhomes_biz" 
    allowed_domains = ["www.apexhomes.biz"]
    start_urls = [
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=detached&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200',
        'property_type':'house'},
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=end-terrace&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200',
        'property_type':'house'},
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=semi-detached&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200',
        'property_type':'house'},
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=terraced&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200',
        'property_type':'house'},
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=flat&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200',
        'property_type':'apartment'},
        {'url':'http://www.apexhomes.biz/properties-search/?keyword=&area=&status=available&type=studio&bedrooms=&bathrooms=&min-price=%C2%A350&max-price=%C2%A31%2C200s',
        'property_type':'studio'}, 
    ]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source="Apexhomes_PySpider_united_kingdom_en"

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[contains(@class,"item-wrap")]')
        for listing in listings:
            property_url = listing.xpath('.//a[@class="hover-effect"]/@href').extract_first()
            room_count = listing.xpath('.//div[@class="cell"]//p/span[contains(@class,"bed")]/text()').extract_first()
            bathroom_count = listing.xpath('.//div[@class="cell"]//p/span[contains(@class,"bath")]/text()').extract_first()
            available_date = listing.xpath('.//a[contains(text(),"AVAILABLE")]/text()').extract_first()
            rent_string = listing.xpath('//span[@class="item-price"]/text()').extract_first()
            try:
                yield scrapy.Request(
                    url=property_url, 
                    callback=self.get_property_details,
                    meta={'request_url':property_url,
                        'property_type':response.meta.get('property_type'),
                        'bathroom_count':bathroom_count,
                        'room_count':room_count,
                        'available_date':available_date,
                        'rent_string':rent_string})
            except: pass
                

        next_page_url = response.xpath('//a[@rel="Next"]/@href').extract_first()
        if next_page_url:
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')})


    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_xpath("external_id", './/strong[contains(text(),"Property ID")]/following-sibling::text()')
        item_loader.add_value("property_type", response.meta.get('property_type'))
        if response.meta.get('room_count'):
            item_loader.add_value("room_count", extract_number_only(response.meta.get('room_count')))
        if response.meta.get('bathroom_count'):
            item_loader.add_value("bathroom_count", extract_number_only(response.meta.get('bathroom_count')))
        
        if response.meta.get('available_date'):
            available_date = response.meta.get('available_date').split(': ')[-1].replace('-',' ')
            import dateparser
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        rent = response.xpath("//strong[contains(.,'Price')]/following-sibling::text()").get()
        if rent and "pw" in rent.lower():
            rent = rent.split("£")[1].split("/")[0].strip()
            item_loader.add_value("rent", int(float(rent))*4)
        else:
            rent = rent.split("£")[1].split("/")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        # if response.meta.get('rent_string'):
        #     rent = int(extract_number_only(response.meta.get('rent_string')))*4
        #     currency = response.meta.get('rent_string')[0]
        #     item_loader.add_value("rent_string", currency+str(rent))

        desc = " ".join(response.xpath('.//div[@id="description"]/p/text()').getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        item_loader.add_xpath("address", './/h1/text()')
        if item_loader.get_output_value("address"):
            item_loader.add_xpath("city", './/strong[contains(text(),"City")]/following-sibling::text()')
            zipcode = item_loader.get_output_value("address").split()[-2:]
            if "leed" not in "".join(zipcode).lower():
                item_loader.add_value("zipcode", zipcode)
        zipcodecheck=item_loader.get_output_value("zipcode")
        if not zipcodecheck: 
            zipcode=response.xpath(".//h1/text()").get()
            if zipcode:
                item_loader.add_value("zipcode",zipcode.split(" ")[-1])


        title = response.xpath("//div[contains(@class,'header-detail')]//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
            
        images = response.xpath('.//div[contains(@style,"background-image")]/@style').extract()
        images = [i[:-1].replace('background-image:url(', '') for i in images]
        item_loader.add_value('images',images)

        javascript = response.xpath('.//script[contains(text(), "property_lat")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            item_loader.add_value('latitude', xml_selector.xpath('.//property[@name="property_lat"]/string/text()').extract_first())
            item_loader.add_value('longitude', xml_selector.xpath('.//property[@name="property_lng"]/string/text()').extract_first())

        terrace_check = response.xpath('.//strong[contains(text(),"Property Type:")]/following-sibling::text()').extract_first()
        if 'terrace' in terrace_check.lower():
            item_loader.add_value("terrace", True)

        item_loader.add_value('landlord_name', 'Apex Homes')
        item_loader.add_value('landlord_email', 'lettings@apexhomes.biz')
        item_loader.add_value('landlord_phone', '0113 2747800')

        item_loader.add_value("external_source", self.external_source)
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()

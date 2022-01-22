# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import itemadapter
import scrapy
from scrapy.http import Request, FormRequest
import re
import js2xml
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import json
import dateparser
def extract_city_zipcode(_address):
    zip_city = _address.split(",")[1].strip()
    zipcode =  zip_city.split(" ")[0].strip()
    city =  zip_city.split(" ")[-1].strip()
    return zipcode,city

class ImmoarnouSpider(scrapy.Spider):
    name = 'immoarnou'
    allowed_domains = ['immo-arnou']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator=','
    scale_separator='.'
    external_source="Immoarnou_PySpider_belgium_nl"

    def start_requests(self):
        start_urls = [
                {
                    'url': [
                    'https://immo-arnou.be/api/estates'
                    ],
                }                

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        for item in data["data"]: 
            followurl=item["url"]
            yield Request(url=followurl, callback=self.get_property_details,meta={"item":item },dont_filter=True)
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.url
        dontallow=response.xpath("//div[@class='estate-label font-supplementary--bold']/text()").get()
        if dontallow and "Verhuurd" in dontallow:
            return 
        item_loader.add_value("external_link",external_link)
        item_loader.add_value('external_source', self.external_source)
        item = response.meta.get('item')      
        item_loader.add_value("title", item["title"]) 
        item_loader.add_value("external_id",item["reference"])
        item_loader.add_value("rent",str(item["price"]).split("€")[-1].strip())
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("latitude",str(item["latitude"]))
        item_loader.add_value("longitude",str(item["longitude"]))
        address=response.xpath("//a[@class='estate-address']/text()").get()
        if address:
            address=address.replace("\n","").strip()
            item_loader.add_value("address",address)
        item_loader.add_value("city",item["location"])
        images=item["images"]
        if images:
            item_loader.add_value("images",item["images"])
        item_loader.add_value("square_meters",item["area"])
        item_loader.add_value("bathroom_count",item["bathrooms"])
        zipcode=response.xpath("//a[@class='estate-address']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[1].strip().split(" ")[0])
        item_loader.add_value("room_count",item["bedrooms"])
        category=item["category"]
        if category:
            if "Appartement" in category:
                item_loader.add_value("property_type","apartment")
            elif "Huis" in category:
                item_loader.add_value("property_type","house")
            elif "Garage" in category:
                return 
        description =response.xpath("//h2[@class='title-as-text']/following-sibling::text()").get()
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        available_date=response.xpath("//div[contains(text(),'Beschikbaarheid')]/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        

            item_loader.add_value('landlord_name', 'Immokantoor Arnou')
            item_loader.add_value('landlord_email', 'info@immo-arnou.be')
            item_loader.add_value('landlord_phone', '09 386 28 52')
    

        yield item_loader.load_item()



         
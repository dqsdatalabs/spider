# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser
class MySpider(Spider):
    name = 'allens_property_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.allens-property.com/search?sta=toLet&st=rent&pt=residential&stygrp=3", "property_type": "apartment"},
	        {"url": "https://www.allens-property.com/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=8&stygrp=6", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(
                url=url.get('url'),
                callback=self.parse,
                meta={'property_type': url.get('property_type')}
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='property-link']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Allens_Property_PySpider_united_kingdom")
        title = " ".join(response.xpath("//h1[@class='address']//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td/text()")
        item_loader.add_xpath("city", "//h1[@class='address']//span[@class='locality']/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
    
        rent_string = " ".join(response.xpath("//tr[th[.='Rent']]/td//text()").getall())
        if rent_string:
            if "week" in rent_string.lower():
                rent = rent_string.split("/")[0].split("£")[-1].replace(",","")
                item_loader.add_value("rent", str(int(rent.strip())*4))
            else:
                item_loader.add_value("rent", rent_string.replace(",",""))
        item_loader.add_value("currency", "GBP")
        zipcode = response.xpath("//h1[@class='address']//span[@class='postcode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.replace(",","").strip())
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(":")[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
      
        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td//a[contains(@href,'/epc/ee/')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
  
        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:
            item_loader.add_value("deposit", int(float(deposit.split("£")[-1].replace(",",".").strip())))

        images = [x for x in response.xpath("//div[@class='slideshow-thumbs']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
   
        item_loader.add_value("landlord_name", "ALLENS ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "028 8676 2233")
        item_loader.add_value("landlord_email", "info@allens-property.com")
        yield item_loader.load_item()
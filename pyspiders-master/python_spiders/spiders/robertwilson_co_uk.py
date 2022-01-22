# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re
import dateparser

class MySpider(Spider):
    name = 'robertwilson_co_uk'
    execution_type='testing'
    country='ireland'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.robertwilson.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.robertwilson.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//section[@class='searchPage-results']//li//a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])

        item_loader.add_value("external_source", "Robertwilson_Co_PySpider_ireland")
        item_loader.add_xpath("title","//title/text()")
        address = " ".join(response.xpath("//h1[@class='Address ']/span//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.replace("\n","").strip())
        zipcode = " ".join(response.xpath("//div[@class='ListingHeadline-address']//span[@class='Address-addressPostcode']/span/text()[normalize-space()]").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//div[@class='ListingHeadline-address']//span[@class='Address-addressTown']//text()").get()
        if city:                   
            item_loader.add_value("city",city.strip())
        rent = "".join(response.xpath("//tr[th[.='Rent']]/td/span/text()[.!='POA']").getall())
        if rent:              
            if "week" in rent:
                rent = rent.lower().split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)*4))) 
            else:
                rent = rent.lower().split('£')[-1].split('/')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)))) 
        item_loader.add_value("currency", 'GBP')

        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:                   
            item_loader.add_value("deposit", deposit.replace(",",""))

        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()") 
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()") 

        description = " ".join(response.xpath("//section[@class='ListingDescr']//text()[.!='Additional Information']").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td/span/a/text()").get()
        if energy_label:                   
            energy =  energy_label[0].strip()
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label",energy)

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@property='og:longitude']/@content")

        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@class='Slideshow-thumbs']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)
        terrace = response.xpath("//tr[th[.='Style']]/td//text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace",True)

        parking = response.xpath("//div[@class='ListingDescr-text']/ul/li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking",True)

        item_loader.add_value("landlord_phone", "028 9261 3100")
        item_loader.add_value("landlord_name", "Robert Wilson Estate Agency Group")
        item_loader.add_value("landlord_email", "moira@robertwilson.co.uk")
 
        yield item_loader.load_item()
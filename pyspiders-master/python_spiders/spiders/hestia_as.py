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

class MySpider(Spider): 
    name = 'hestia_as' 
    execution_type='testing'
    country='denmark'
    locale='da'
    external_source="Hestia_PySpider_denmark" 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hestia.as/ledige-lejligheder/",
                ],
                "property_type" : "apartment",
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='tb-button__link']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title","//title//text()")

        rent=response.xpath("//p[.='Husleje']/parent::div/parent::div/following-sibling::div/div/p/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split(",")[0].replace(".",""))

        images=[x for x in response.xpath("//ul[@class='glide__slides']//li//img//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        room_count=response.xpath("//p[.='Antal værelser']/parent::div/parent::div/following-sibling::div/div/p/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=response.xpath("//p[.='Størrelse']/parent::div/parent::div/following-sibling::div/div/p/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m2")[0])
        adres=item_loader.get_output_value("title")
        if adres:
            item_loader.add_value("address",adres.split("-")[0])
        zipcode=item_loader.get_output_value("title")
        if zipcode:
            zipcode=re.findall("\d{4}",zipcode)
            item_loader.add_value("zipcode",zipcode)
        import dateparser
        available_date=response.xpath("//p[.='Overtagelse']/parent::div/parent::div/following-sibling::div/div/p/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        city = response.xpath("//div/h4/text()").get()
        if city:
            city = city.split()[1].strip()
            item_loader.add_value("city",city)

        external_id = response.xpath("//div[p[text()='Sags nr.']]/following::div//p/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip("#"))

        deposit = response.xpath("//div[p[text()='Depositum']]/following::div//p/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.strip())      

        utilities = response.xpath("//div[p[text()='Indvendig vedligeholdelse']]/following::div//p/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.strip())   

        item_loader.add_value("landlord_email","ledigelejligheder@hestia.as")
        item_loader.add_value("landlord_phone","1015 005 003")
        item_loader.add_value("landlord_name","Hestia Ejendomme")
        item_loader.add_value("currency","DKK")

        yield item_loader.load_item()
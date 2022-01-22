# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 

class MySpider(Spider):
    name = 'trianon-invest_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Trianoninvest_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        payload = {"form":{"minRooms":"0"}}
        yield Request(
            "https://www.trianon-invest.be/index.php?option=com_izimo&view=ajax&layout=get-estates-db&wlang=en-GB&itemid=273&lang=en&page=1&itemsperpage=9&orderby=putonline_at&ascordesc=desc",
            callback=self.parse,
            body=json.dumps(payload),
            method="POST",
            dont_filter=True,
        )
    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)['data']
        page = response.meta.get("page", 2)
        seen = False
        for item in data:
            follow_url = item['joomlaURI']
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page==2 or seen:
            nextpage=f"https://www.trianon-invest.be/index.php?option=com_izimo&view=ajax&layout=get-estates-db&wlang=en-GB&itemid=273&lang=en&page={page}&itemsperpage=9&orderby=putonline_at&ascordesc=desc"
            if nextpage:
                yield Request(nextpage, callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//span[.='Type']/following-sibling::span/text()").get()
        if property_type and "Flat"==property_type:
            item_loader.add_value("property_type","apartment")
        if "house" in response.url:
            item_loader.add_value("property_type","house")
        adres=response.xpath("//span[@class='estate-addr']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        if not adres:
            item_loader.add_value("address","Bruxelles")
        external_id=response.xpath("//span[.='Reference']/following-sibling::span/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        zipcode=response.xpath("//span[.='Zip']/following-sibling::span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        bathroom_count=response.xpath("//span[.='Bathrooms']/following-sibling::span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        dontallow=response.url
        if dontallow and "offices" in dontallow:
            return 
        room_count=response.xpath("//span[.='Bedrooms']/following-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        if "studio" in response.url:
            item_loader.add_value("room_count","1")
        square_meters=response.xpath("//span[.='Living  surface']/following-sibling::span/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        floor=response.xpath("//span[.='Etage']/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor",floor)
        description=response.xpath("//h2[.='Estate description']/following-sibling::p/text()").get()
        if description:
            item_loader.add_value("description",description)
        energy_label=response.xpath("//h2[.='Estate description']/following-sibling::p/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split("PEB:")[-1].split("+")[0])
        rent=response.xpath("//span[@class='single-estate-price price pull-left']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].replace(" ","").replace("\n","").replace("\xa0","").strip())
        item_loader.add_value("currency","EUR")
        utilities=response.xpath("//span[.='Charges (€)']/parent::div/following-sibling::div/span/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].split(",")[0])
        deposit=response.xpath("//span[.='Rental guarantee']/parent::div/following-sibling::div/span/text()").get()
        if deposit:
            rent=rent.split("€")[0].replace(" ","").replace("\n","").replace("\xa0","").strip()
            deposit=int(deposit)*rent
            item_loader.add_value("deposit",deposit)
        terrace=response.xpath("//span[.='Terrace 1']").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking=response.xpath("//span[.='Inside parking']/parent::div/following-sibling::div/span/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        elevator=response.xpath("//span[.='Inside parking']/parent::div/following-sibling::div//span[2]/text()").get()
        if elevator and "Yes"==elevator:
            item_loader.add_value("elevator",True)
        images=[x for x in response.xpath("//img[@class='img-responsive']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_email","info@trianon-invest.be")
        item_loader.add_value("landlord_phone","02 340 37 07")
        yield item_loader.load_item()
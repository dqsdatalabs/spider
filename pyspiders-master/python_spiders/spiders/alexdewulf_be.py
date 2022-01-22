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
    name = 'alexdewulf_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Alexdewulf_PySpider_belgium'
    start_urls = ["https://www.alexdewulf.be/nl/huren/Loadmore?prijsklassetehuur=0&categorie=0&regio=0&search="]
    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.xpath("//a[@class='pand']/@href").getall():
            yield Request(response.urljoin(follow_url), callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        property_type=response.xpath("//h4[.='Subcategorie']/following-sibling::div/text()").get()
        if property_type and property_type=="appartement":
            item_loader.add_value("property_type","apartment")
        if property_type and ("garage" in property_type or "handelszaak" in property_type):
            return 
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        description=response.xpath("//div[@class='beschrijving fontweight_300']/p/text()").get()
        if description:
            item_loader.add_value("description",description)
        rent=response.xpath("//div[@class='vraagprijs fontweight_700 textcolor_gold fontsize_30']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(".","").replace(" ",""))
        furnished=response.xpath("//h4[.='Gemeubeld']/following-sibling::div/text()").get()
        if furnished and "Ja"==furnished:
            item_loader.add_value("furnished",True)
        elevator=response.xpath("//h4[.='lift']/following-sibling::div/text()").get()
        if elevator and "Ja"==elevator:
            item_loader.add_value("elevator",True)
        parking=response.xpath("//h4[.='parking binnen (aantal)']").get()
        if parking:
            item_loader.add_value("parking",True)
        images=[x for x in response.xpath("//img[contains(@src,'www.alexdewulf.be/CMS_pictures')]/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        square_meters=response.xpath("//span[contains(.,'Oppervlakte')]/parent::div/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        room_count=response.xpath("//span[contains(.,'Slaapkamer')]/parent::div/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count=response.xpath("//span[contains(.,'Badkamers')]/parent::div/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        external_id=response.xpath("//h2[.='Referentie']/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        adres=response.xpath("//div[@class=' beschrijving fontsize_24 fontweight_300 lineheight_40 ']/text()").get()
        if adres:
            item_loader.add_value("address",adres.replace("in","").strip())
        item_loader.add_value("landlord_name","Alex Dewulf")
        item_loader.add_value("landlord_phone","+32 (0)50 623 623")
        item_loader.add_value("landlord_email","alex@alexdewulf.be")



        yield item_loader.load_item() 
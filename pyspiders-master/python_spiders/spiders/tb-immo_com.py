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
    name = 'tb-immo_com'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Tbimmo_PySpider_belgium'
    start_urls = ["http://tb-immo.com/a-louer/"]
    # 1. FOLLOWING
    def parse(self, response):
        for follow_url in response.xpath("//a[@class='omniboxlink Appartements']/@href | //a[@class='omniboxlink Maisons']/@href").getall():
            yield Request(response.urljoin(follow_url), callback=self.populate_item)
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//p[@class='subtitle']/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=title
        if property_type and "villa" in property_type.lower():
            item_loader.add_value("property_type","house")
        if property_type and "appartement" in property_type.lower():
            item_loader.add_value("property_type","apartment")
        rent=response.xpath("//span[@class='priceformat']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].strip().replace(".",""))
        item_loader.add_value("currency","EUR")
        adres="".join(response.xpath("//p[@class='subtitle']/following-sibling::a/text()").getall())
        if adres:
            item_loader.add_value("address",adres)
        latitude=response.xpath("//p[@class='subtitle']/following-sibling::a/@href").get()
        if latitude:
            item_loader.add_value("latitude",latitude.split("place/")[-1].split(",")[0])
        longitude=response.xpath("//p[@class='subtitle']/following-sibling::a/@href").get()
        if longitude:
            item_loader.add_value("longitude",longitude.split("place/")[-1].split(",")[-1])
        images=[x for x in response.xpath("//div[@class='Wallop-list']//a/div/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description="".join(response.xpath("//div[@id='detdescbox']//div[@align='left']/font/span/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//text()[contains(.,'Chambre(s) ')]").get()
        if room_count:
            item_loader.add_value("room_count",room_count.split(":")[-1].strip())
        bathroom_count=response.xpath("//text()[contains(.,'Salle(s) de bains')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.split(":")[-1].strip())
        square_meters="".join(response.xpath("//text()[contains(.,'Superficie habitable')]").getall())
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split(":")[-1].split(".")[0].split("m²")[0].strip().split(",")[0])
        energy_label=response.xpath("//h5[.='PERFORMANCE ÉNERGÉTIQUE']/following-sibling::text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label.split(":")[-1].replace("+",""))
        elevator=response.xpath("//text()[contains(.,'Ascenseur')]").get()
        if elevator and "Oui" in elevator:
            item_loader.add_value("elevator",True)
        utilities=response.xpath("//text()[contains(.,'Charges')]").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("€")[0].strip())
        deposit=response.xpath("//text()[contains(.,'Garantie locative')]").get()
        if deposit:
            deposit=deposit.split(":")[-1].split("mois")[0].strip()
            if deposit:
                rent=item_loader.get_output_value("rent")
                if rent:
                    item_loader.add_value("deposit",int(rent)*int(deposit))
        available_date=response.xpath("//text()[contains(.,'Date de validité ')]").get()
        if available_date:
            item_loader.add_value("available_date",available_date.split(":")[-1].strip())
        item_loader.add_value("landlord_name","TB IMMO")
        yield item_loader.load_item() 
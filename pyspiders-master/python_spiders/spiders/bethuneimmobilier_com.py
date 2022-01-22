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
    name = 'bethuneimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bethuneimmobilier.com/location-bethune-bruay?type=louer&bien=appartement&ville[]&ville[]=ALLOUAGNE&ville[]=ANNEQUIN&ville[]=AUCHEL&ville[]=BAILLEUL-LES-PERNES&ville[]=BARLIN&ville[]=BETHUNE&ville[]=BEUVRY&ville[]=BRUAY-LA-BUISSIERE&ville[]=CAMBLAIN-CHATELAIN&ville[]=CHOCQUES&ville[]=DIEVAL&ville[]=DIVION&ville[]=FESTUBERT&ville[]=FOUQUIERES-LES-BETHUNE&ville[]=HOUDAIN&ville[]=ISBERGUES&ville[]=LA-GORGUE&ville[]=LABEUVRIERE&ville[]=LAPUGNOY&ville[]=LENS&ville[]=LESTREM&ville[]=LILLERS&ville[]=MARLES-LES-MINES&ville[]=MAZINGARBE&ville[]=NOEUX-LES-MINES&ville[]=OURTON&ville[]=REBREUVE-RANCHICOURT&ville[]=RUITZ&ville[]=SAILLY-LABOURSE&ville[]=ST-VENANT&ville[]=VENDIN-LES-BETHUNE&ville[]=VERQUIN",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.bethuneimmobilier.com/location-bethune-bruay?type=louer&bien=maison&ville[]&ville[]=ALLOUAGNE&ville[]=ANNEQUIN&ville[]=AUCHEL&ville[]=BAILLEUL-LES-PERNES&ville[]=BARLIN&ville[]=BETHUNE&ville[]=BEUVRY&ville[]=BRUAY-LA-BUISSIERE&ville[]=CAMBLAIN-CHATELAIN&ville[]=CHOCQUES&ville[]=DIEVAL&ville[]=DIVION&ville[]=FESTUBERT&ville[]=FOUQUIERES-LES-BETHUNE&ville[]=HOUDAIN&ville[]=ISBERGUES&ville[]=LA-GORGUE&ville[]=LABEUVRIERE&ville[]=LAPUGNOY&ville[]=LENS&ville[]=LESTREM&ville[]=LILLERS&ville[]=MARLES-LES-MINES&ville[]=MAZINGARBE&ville[]=NOEUX-LES-MINES&ville[]=OURTON&ville[]=REBREUVE-RANCHICOURT&ville[]=RUITZ&ville[]=SAILLY-LABOURSE&ville[]=ST-VENANT&ville[]=VENDIN-LES-BETHUNE&ville[]=VERQUIN",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base": item})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='TitreBlocInfosAnnonces']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='paginationNext']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Bethuneimmobilier_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//h2/a/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//a[contains(.,'Secteur')]/span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//a[contains(.,'Secteur')]/span/text()", input_type="F_XPATH", split_list={"(":1,")":0})
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//span[@class='villeVente']/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//section[@class='prix']/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(.,'garantie')]/span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//section[@class='surface']/text()", input_type="M_XPATH", get_num=True, split_list={"m":0})
        
        if response.xpath("//li[contains(.,'chambre')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//li[contains(.,'chambre')]/text(),':')", input_type="F_XPATH", get_num=True)
        elif response.xpath("//li[contains(.,'pièce')]/text()").get():
            ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="substring-after(//li[contains(.,'pièce')]/text(),':')", input_type="F_XPATH", get_num=True)
        
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//section[contains(.,'Réf.')][1]/text()", input_type="F_XPATH", split_list={".":1," - ":0})
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"c_lat =":1,";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'lng')]/text()", input_type="F_XPATH", split_list={"c_lng =":1,";":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='sliderAnnonceViewSmall']//@src", input_type="M_XPATH")
        
        energy_label = response.xpath("//div[@class='bilan']//@src[contains(.,'BE')]").get()
        if energy_label:
            energy_label = energy_label.split("BE/")[1].split(".")[0]
            item_loader.add_value("energy_label", energy_label)
        
        desc = " ".join(response.xpath("//section[@class='infoSupp']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        import dateparser
        available_date = response.xpath("//section[@class='detailsLocation']//text()").get()
        if available_date and "Disponible" in available_date:
            available_date = available_date.split(" le ")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="BÉTHUNE IMMOBILIER", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="03 21 01 26 26", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="location@bethuneimmobilier.com", input_type="VALUE")

        yield item_loader.load_item()
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
from python_spiders.helper import ItemClear


class MySpider(Spider):
    name = 'agenceduchateau_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Agenceduchateau_PySpider_france'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.agenceduchateau.fr/biens-immobiliers?property_type%5B%5D=1&reference=&price%5B%5D=0&estate_i18n_search_sended=1&estate_i18n_search_submit=Rechercher",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.agenceduchateau.fr/biens-immobiliers?property_type%5B%5D=15&reference=&price%5B%5D=0&estate_i18n_search_sended=1&estate_i18n_search_submit=Rechercher",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='bienBlocTexte']"):
            price = int(item.xpath(".//p[@class='bienPrix']/strong/text()").get().split("€")[0].replace(" ", ""))
            if price > 5000:
                continue
            follow_url = response.urljoin(item.xpath(".//h3[@class='bienTitre']/a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        # next_page = response.xpath("//a[@class='pagination_grp']/@href").get()
        # if next_page:
        #     p_url = response.urljoin(next_page)
        #     yield Request(
        #         p_url,
        #         callback=self.parse,
        #         meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        title = "".join(response.xpath("(//p[@class='ItemEstateTitle']/text())[1]").extract())
        if title:
            item_loader.add_value("title", title.strip())

        rent = "".join(response.xpath("(//p[@class='prixBien']/text())[1]").extract())
        if title:
            item_loader.add_value("rent", rent.replace("€", "").strip())
        
        room_count = "".join(response.xpath("(//li[@class='bienPiece']/text())[1]").extract())
        if room_count:
            item_loader.add_value("room_count", room_count.replace("pièces", "").strip())
            
        square_meters = "".join(response.xpath("(//li[@class='bienSurfacePiece']/text())[1]").extract())
        if square_meters:
            item_loader.add_value("square_meters", square_meters.replace("m²", "").strip())

        address = "".join(response.xpath("//li[@class='bienLocalisation']/text()").extract())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.strip())

        desc = "".join(response.xpath("//div[@class='bienEncart']/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if desc and "de bain" in desc.lower():
                item_loader.add_value("bathroom_count", 1)
            if desc and "terrasse" in desc.lower():
                item_loader.add_value("terrace", True)
        
        energy_label = "".join(response.xpath("//p[@class='intitule']/img/@src").extract())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('/dpe-')[1].split('-')[0].upper())
        
        images = [x for x in response.xpath("//ul[@class='splide__list']//a//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        item_loader.add_value("landlord_name", "Agence du Château")
        item_loader.add_value("landlord_phone", "02 38 36 23 62")
        item_loader.add_value("landlord_email", "contact@agenceduchateau.fr")
        
        yield item_loader.load_item()
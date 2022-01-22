# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'inexia_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.inexia-immo.fr/ajax/ListeBien.php?page=1&menuSave=2&ListeViewBienForm=text&ope=2&filtre=2&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=394&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.inexia-immo.fr/ajax/ListeBien.php?menuSave=2&page=1&ListeViewBienForm=text&ope=2&filtre=8&lieu-alentour=0&langue=fr&MapWidth=100&MapHeight=394&DataConfig=JsConfig.GGMap.Liste&Pagination=0",
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
        page = response.meta.get("page", 2)
        for item in response.xpath("//div[contains(@class,'liste-bien-photo-frame')]/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_page = response.xpath("//span[@class='PageSui']/@id").get()
        if next_page:
            p_url = f"https://www.inexia-immo.fr/fr/liste.htm?page={page}&menuSave=2&ListeViewBienForm=text&ope=2&filtre=2&lieu-alentour=0"
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Inexia_Immo_PySpider_france") 
        item_loader.add_xpath("title", "//title/text()") 
        item_loader.add_xpath("external_id", "//li[span[.='Ref']][2]/span[@itemprop='productID']/text()") 
             
        city = "".join(response.xpath("//li[span[.='Ville']]/text()").extract())
        if city:
            item_loader.add_value("address", city.strip())
            item_loader.add_value("city", city.strip())

        zipcode = response.xpath("substring-before(substring-after(//meta[@name='Description']/@content,'('),')')").get()
        if zipcode and zipcode.isdigit() and len(zipcode)==5:
            item_loader.add_value("zipcode", zipcode)
        
        meters = "".join(response.xpath("//li[span[.='Surface']]/text()[.!=' NC']").extract())
        if meters:
            if  meters is not None:
                item_loader.add_value("square_meters", meters.split("m²")[0].strip())

        rent = "".join(response.xpath("//li[@class='prix']/text()").extract())
        if rent:
            price = rent.split(":")[1].strip().split("€")[0].strip()
            item_loader.add_value("rent", price.strip())
            item_loader.add_value("currency", "EUR")

        item_loader.add_xpath("utilities", "//span[@class='cout_charges_mens']/text()")
        item_loader.add_xpath("deposit", "//span[contains(.,'Dépôt')]/following-sibling::span/text()")

        room = "".join(response.xpath("//li[span[.='Pièces']]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())
        else:
            room = "".join(response.xpath("//li[span[.='Chambres']]/text()").extract())
            if room:
                item_loader.add_value("room_count", room.strip())
        


        description = " ".join(response.xpath("//div[@class='detail-bien-desc-content']//p[@itemprop='name']/text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@class='diapo is-flap']/img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_xpath("latitude", "//li[@class='gg-map-marker-lat']/text()")
        item_loader.add_xpath("longitude", "//li[@class='gg-map-marker-lng']/text()")

        item_loader.add_xpath("energy_label", "substring-before(substring-after(//div[@class='detail-bien-dpe']/img/@src[contains(.,'nrj')],'consommation-'),'.')")

        available_date = "".join(response.xpath("//p[@itemprop='name']/text()[contains(.,'DISPONIBILITE')]").extract())
        if available_date:
            date2 =  available_date.split(":")[1].replace("A PARTIR DU","").replace("IMMEDIATE","now").replace("A PARTIR DE","").replace("LIVRAISON PREVUE LE","").replace("Bien meublé.","").replace("A PARIT DU","").strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        item_loader.add_value("landlord_phone", "05 61 24 76 92")
        item_loader.add_value("landlord_name", "INEXIA-IMMO")
        item_loader.add_value("landlord_email", "contact@inexia-immo.fr")
        
        yield item_loader.load_item()
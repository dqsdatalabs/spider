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
    name = 'berbudeau_immobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.berbudeau-immobilier.com/immobilier/location-type/appartement-categorie/1p-pieces/1.html",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.berbudeau-immobilier.com/immobilier/location-type/maison-categorie/2p-chambres/1.html",
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
        seen = False

        for item in response.xpath("//article[@class='liste-item']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace(str(page - 1) + ".html", str(page) + ".html")
            yield Request(
                follow_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "Berbudeau_Immobilier_PySpider_france")     
        title = " ".join(response.xpath("//h3[@class='detail-offre-titre']//text()").getall())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
        address = " ".join(response.xpath("//h2[@class='detail-header-titre']/small/text()").getall())
        if address:
            city = address.split(" - ")[0].strip()
            item_loader.add_value("address", city) 
            item_loader.add_value("zipcode", city.split("(")[1].split(")")[0]) 
            item_loader.add_value("city", city.split("(")[0].strip()) 
            ext_id = address.split("réf.")[-1].strip()
            item_loader.add_value("external_id", ext_id.strip()) 

        room_count = response.xpath("//div[@class='detail-offre-caracteristiques']//li[contains(.,'chambr')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0].strip().split(" ")[-1])
        else:
            room_count = response.xpath("//h3[@class='detail-offre-titre']//text()[contains(.,'pièce')]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split("pièce")[0].strip().split(" ")[-1])
        bathroom_count = response.xpath("//li[contains(.,'salle de ')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("salle")[0].strip().split(" ")[-1])

        terrace = response.xpath("//div[@class='detail-offre-caracteristiques']//li[contains(.,'terrasse')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//div[@class='detail-offre-caracteristiques']//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
 
        square_meters = response.xpath("//h3[@class='detail-offre-titre']//text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip().split(" ")[-1])
      
        description = " ".join(response.xpath("//p[@class='detail-offre-texte']//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='gallery2']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        rent =" ".join(response.xpath("//p[@class='detail-offre-prix']//text()").getall())
        if rent:
            item_loader.add_value("rent_string", rent.strip().replace(" ",""))
  
        deposit = response.xpath("//li[contains(.,'dépôt de garantie')]//text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("dépôt de garantie")[0].split(",")[-1].replace("\xa0",""))
            
        utilities = response.xpath("//li//text()[contains(.,'de provision')]").get()
        if utilities:
            utilities = utilities.split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilities)
        
        energy_label = response.xpath("//h5[@class='panel-title']//text()[contains(.,'(dpe)')]").get()
        if energy_label:
            energy = energy_label.split("(dpe)")[-1].split("- ")[0].strip()
            if energy in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy )
        item_loader.add_xpath("latitude", "//div[@id='collapse1']/@data-latgps")
        item_loader.add_xpath("longitude", "//div[@id='collapse1']/@data-longgps")
        item_loader.add_value("landlord_name", "Berbudeau Immobilier")
        item_loader.add_value("landlord_phone", "04 90 67 67 20")
        item_loader.add_value("landlord_email", "berbudeau@berbudeau-immobilier.com")
     
        yield item_loader.load_item()
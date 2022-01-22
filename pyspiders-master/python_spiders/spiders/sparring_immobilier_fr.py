# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'sparring_immobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Sparring_Immobilier_PySpider_france'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.sparring-immobilier.fr/tous-nos-biens/?type_bien=appartement&ville=0&type_mandat=location&budget_maxi=&surface_mini=&pieces_mini=&post_type=bien",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.sparring-immobilier.fr/tous-nos-biens/?type_bien=maison&ville=0&type_mandat=location&budget_maxi=&surface_mini=&pieces_mini=&post_type=bien",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
    
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'bien') and contains(@class,'row')]/article/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})

        next_button = response.xpath("//a[contains(@class,'next') and contains(@class,'page-link')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type": response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_xpath("energy_label", "//tr[th[.='Consommation énergétique']]/td/text()[.!='NI ']")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Nombre de salle de bains']]/td/text()")
        item_loader.add_xpath("floor",'//tr[th[.="Numéro de l\'étage"]]/td/text()')
        square_meters = response.xpath("//tr[th[.='Surface habitable']]/td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
        room_count = response.xpath("//tr[th[.='Nombre de chambres']]/td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        rent_string = response.xpath("//tr[th[.='Prix']]/td/span/text()").get()
        if rent_string:
            rent_string = rent_string.split('€')[0].strip().replace(" ","")
            if int(rent_string) >= 12000:
                return
            else:    
                item_loader.add_value("rent_string", rent_string)
        item_loader.add_value("currency", "EUR")
        address = response.xpath("//div[@id='map-bien']/iframe/@src").get()
        if address:
            address = address.split("&q=")[-1].split("&")[0]
            if address.isalpha():
                item_loader.add_value("address", address)
                item_loader.add_value("city", address)

        desc="".join(response.xpath("//div[@class='descriptif-texte']/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        utilities = response.xpath("//div[@class='descriptif-texte']/p//text()[contains(.,' charges') and contains(.,'+')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("+")[1].split(" charges")[0].strip())
        images=[x for x in response.xpath("//ul[@id='imageGallery']/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
   
        furnished = response.xpath("//tr[th[.='Meublé']]/td/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "oui" in furnished.lower():
                item_loader.add_value("furnished", True)
        elevator = response.xpath("//tr[th[.='ASCENSEUR']]/td/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            elif "oui" in elevator.lower():
                item_loader.add_value("elevator", True)
    
        item_loader.add_value("landlord_name", "SPARRING-IMMOBILIER")
        item_loader.add_value("landlord_phone", "05 63 03 81 40")
        
        yield item_loader.load_item()
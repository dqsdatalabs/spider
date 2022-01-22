# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'caractereimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Caractereimmo_PySpider_france"
    custom_settings = {"HTTPCACHE_ENABLED":False}
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.caractereimmo.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=To+research",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.caractereimmo.com/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=To+research",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            dont_filter=True,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//p[@class='lien-detail']/a/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        # if page == 2 or seen:
        #     base = response.meta["base"]
        #     p_url = base.format(page)
        #     yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page":page + 1, "base":base})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")

        external_id = response.xpath("//p[@class='header-ref']/text()").get()
        if external_id:
            external_id = external_id.split(":")[-1].strip()
            item_loader.add_value("external_id", external_id)
   
        city = response.xpath("//li[contains(text(),'Ville')]/strong/text()").get()
        if city:
            item_loader.add_value("address", city)
            if "lyon" in city.lower():
                item_loader.add_value("city", "Lyon")
            else:
                item_loader.add_value("city", city)

        item_loader.add_xpath("floor", "//li[contains(text(),'Etage')]/strong/text()")

        bathroom_count = response.xpath("//li[contains(text(),'salle de bain')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        else:
            item_loader.add_xpath("bathroom_count", "//div[span[contains(.,'Salle d')]]/span[2]/text()")

        room_count = response.xpath("//li[contains(text(),'pièces')]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[span[.='Pièce(s)']]/span[2]/text()")
      
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//li[contains(text(),'Carrez')]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(",")[0].strip())
        rent = response.xpath("//li[contains(text(),'Loyer')]/strong/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.replace(" ",""))
        deposit = response.xpath("//li[contains(text(),'charge du')]/strong/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ","").replace("€",""))
        utilities = response.xpath("//li[contains(text(),'réalisation d’état')]/strong/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.replace(" ","").replace("€",""))
        lat_lng = response.xpath("//a[@class='popup-gmaps']/@href").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split("center=")[-1].split(",")[0])
            item_loader.add_value("longitude", lat_lng.split("center=")[-1].split(",")[-1])
       
        images = [response.urljoin(x) for x in response.xpath("//div[@class='carousel-inner']/div//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
            
        item_loader.add_value("landlord_name", "CARACTÈRE IMMOBILIER")
        item_loader.add_value("landlord_phone", "04 78 52 06 95")

        
        furnished = response.xpath("//li[contains(text(),'Meublé')]/strong/text()").get()
        if furnished:
            item_loader.add_value("furnished",True)

        balcony = response.xpath("//li[contains(text(),'Balcon')]/strong/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)

        elevator = response.xpath("//li[contains(text(),'Ascenseur')]/strong/text()").get()
        if elevator:
            item_loader.add_value("elevator",True)

        terrace = response.xpath("//li[contains(text(),'Terrasse')]/strong/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)

        yield item_loader.load_item()

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label
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
    name = 'lille_immo_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr' 
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lille-immo.fr/produits.php?ff=hab&transaction_hab=L&type_hab%5B%5D=A&type_hab%5B%5D=T1&type_hab%5B%5D=T2&type_hab%5B%5D=T3&type_hab%5B%5D=T4&type_hab%5B%5D=T5&type_hab%5B%5D=D&type_hab%5B%5D=L&ville_hab=LILLE&min_price_loc_hab=0&max_price_loc_hab=2+500",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lille-immo.fr/produits.php?ff=hab&transaction_hab=L&type_hab%5B%5D=M&ville_hab=LILLE&min_price_loc_hab=0&max_price_loc_hab=2+500",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.lille-immo.fr/produits.php?ff=hab&transaction_hab=L&type_hab%5B%5D=S&ville_hab=LILLE&min_price_loc_hab=0&max_price_loc_hab=2+500",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "url":item})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[@class='desc']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        

        if page == 2 or seen:
            p_url = f"https://www.lille-immo.fr/produits.php?page={page}"
            headers = {
                "Referer": response.meta.get("url"),
                "Host": "www.lille-immo.fr",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "Connection": "keep-alive",
            }
            yield Request(
                p_url,
                callback=self.parse,
                headers=headers,
                meta={'property_type': response.meta['property_type'], "page":page+1, "url":response.meta.get("url")}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Lille_Immo_PySpider_france")
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = "".join(response.xpath("//h1[contains(@class,'main_title')]/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip().split(".")[1].strip())

        rent = "".join(response.xpath("(//h1[@class='main_title small_title light_blue']/span/text()[contains(.,'€')])[1]").getall())
        if rent:
            price = rent.split('•')[-1].strip().split('€')[0].replace(" ","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        utilities = "".join(response.xpath("substring-after(//div[@class='txt_desc']/text()[contains(.,'Charges')],'Charges : ')").getall())
        if utilities:
            item_loader.add_value("utilities", utilities.strip().split("€")[0])

        available_date=response.xpath("substring-after(//div[@class='txt_desc']/text()[contains(.,'Disponibilité')],'Disponibilité : ')").get()
        if available_date:
            date2 =  available_date.split("Mandat")[0].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            if date_parsed:
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[@class='txt_desc']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        room = ""
        room_count = " ".join(response.xpath("//ul[contains(@class,'list_bien')]/li/p[contains(.,'pièces')]/text()").getall()).strip()   
        if room_count:
            room = room_count.split(" ")[0].strip()

        else:
            room_count = " ".join(response.xpath("//ul[contains(@class,'list_bien')]/li/p[contains(.,'chambres')]/text()").getall()).strip()   
            if room_count:
                room = room_count.split(" ")[0].strip()

        item_loader.add_value("room_count", room.strip())


        square_meters = " ".join(response.xpath("//ul[contains(@class,'list_bien')]/li/p[contains(.,'m²')]/text()").getall()).strip()   
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip())

        energy_label = " ".join(response.xpath("//div[@class='diag nrj']/span/@data-value").getall()).strip()   
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))

        images = [ x for x in response.xpath("//ul[@class='owl-carousel']/li/a/@href[contains(.,'.jpg')]").getall()]
        if images:
            item_loader.add_value("images", images)

        address = " ".join(response.xpath("substring-after(//h1/span/text(),'à ')").getall()).strip()   
        if address:
            city = address.replace("•","")
            item_loader.add_value("address", city.split(" ")[0])
            item_loader.add_value("city", address.split("•")[0])
        
        furnished = response.xpath("//h2[contains(.,' meubl')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_phone", "03 20 15 20 61")
        item_loader.add_value("landlord_name", "Estelle DENIS")
        item_loader.add_value("landlord_email", "contact@lille-immo.fr")

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
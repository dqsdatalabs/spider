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
    name = 'locagestion_com'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source="Locagestion_PySpider_france"
    start_urls = ['https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=&loyer-min=&loyer-max=']  # LEVEL 1

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+%C3%A0+r%C3%A9nover&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+ancien&loyer-min=&loyer-max=    ",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+bourgeois&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+neuf&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+r%C3%A9cent&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Appartement+r%C3%A9nov%C3%A9&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T1&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T1+bis&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T2&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T3&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T4&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T5&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=T6+et+plus&loyer-min=&loyer-max=",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Bungalow&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Corps+de+ferme&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Duplex%2FTriplex&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Loft&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+ancienne&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+bourgeoise&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+d%27architecte&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+de+bourg&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+de+campagne&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+de+ma%C3%AEtre&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+de+village&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+de+ville&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+en+bois&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+en+pierres&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+en+pierres&loyer-min=&loyer-max=",
                    #"https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+en+rang%C3%A9e&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+individuelle&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+jumel%C3%A9e&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+neuve&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+r%C3%A9cente&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+situ%C3%A9e+en+campagne&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Maison+traditionnelle&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Penthouse&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Villa&loyer-min=&loyer-max=",
                ],
                "property_type": "house"
            },
            {
                "url": [
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Chambre&loyer-min=&loyer-max=",
                    "https://www.locagestion.com/nos-biens.php?la_page=1&trier=updated_at&search=&type-logement=Studio&loyer-min=&loyer-max=",
                    
                ],
                "property_type": "studio"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//div[contains(@class,'item wrapper-product')]"):
            follow_url = item.xpath(".//a/@href").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"la_page={page-1}", f"la_page={page}")
            yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)

        externalid=response.url.split("-")[-1].split(".")[0]
        if externalid:
            item_loader.add_value("external_id", externalid)
        title ="".join(response.xpath("//title//text()").get())
        if title:
            title=title.replace("\n","").replace("\t","")
            title=re.sub('\s{2,}',' ',title.strip().replace("Locagestion",""))
            item_loader.add_value("title", title)
        address =response.xpath("//li[last()]//span[@itemprop='name']/text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(" ")[-1]
            item_loader.add_value("city", city.strip()) 
            externalid=item_loader.get_output_value("external_id")
            if externalid and externalid=='99180':
                item_loader.add_value("city","Paris") 
        zipcode=response.xpath("//h1[@class='h1-product']/span[2]/text()").get()
        if zipcode:
            zipcode=zipcode.split("(")[-1].split(")")[0]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        rent =response.xpath("//p[@class='loyer']/strong/text()").get()
        if rent:
            rent=rent.split("€")[0].split(".")[0]
            if int(rent) >= 12000:
                return
            else:
                item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "EUR")  
        room_count =response.xpath("//li/span[contains(.,'pièces') or contains(.,'pièce')]/text()").get()
        if room_count:
            room_count =re.findall("\d+",room_count)
            item_loader.add_value("room_count", room_count)
        bathroom_count =response.xpath("//span[contains(.,'salles de bain')]/text() | //span[contains(.,'salle de bain')]/text()").get()
        if bathroom_count:
            bathroom_count =re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)

        description ="".join(response.xpath("//h2[@itemprop='description']/text() | //p//strong/following-sibling::text()").getall())
        if description:
            item_loader.add_value("description", description.strip().replace("Locagestion",""))
        images = [x for x in response.xpath("//div[contains(@class,'swiper-slide')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        squaremeters=response.xpath("//span[contains(text(),'Surface')]/text()").get()
        if squaremeters:
            squaremeters=re.findall("\d+",squaremeters)
            item_loader.add_value("square_meters", squaremeters)
        features =response.xpath("//ul[@class='pt-30']//li//text()").getall()
        if features:
            for i in features:
                if "garage" in i.lower() or "parking" in i.lower():
                    item_loader.add_value("parking", True)  
                if "terrace" in i.lower() or "gardien" in i.lower():
                    item_loader.add_value("terrace", True) 
                if "balcony" in i.lower():
                    item_loader.add_value("balcony", True)
                if "furnished" in i.lower() or "meublé" in i.lower():
                    item_loader.add_value("furnished", True)
                if "ascenseur" in i.lower():
                    item_loader.add_value("elevator", True)
        utilities=response.xpath("//span[contains(text(),'Charges')]/strong/text()").get()
        if utilities:
            utilities=utilities.split(".")[0].split("\xa0")[0].split("€")[0]
            item_loader.add_value("utilities",utilities)
        deposit=response.xpath("//span[contains(text(),'Dépôt de garantie')]/strong/text()").get()
        if deposit:
            deposit=deposit.split(".")[0].split("\xa0")[0].split("€")[0]
            item_loader.add_value("deposit",deposit)
        energylabel=response.xpath("//span[contains(text(),'DPE')]/strong/text()").get()
        if energylabel:
            energylabel=energylabel.split("(")[0].strip()
            item_loader.add_value("energy_label",energylabel)
        item_loader.add_value("landlord_name","LA SOLUTION GESTION")
        phone=response.xpath("//p[@class='tel']/text()").get()
        if phone:
            phone=phone.replace("\n","").replace("\t","").strip()
            item_loader.add_value("landlord_phone",phone)
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            phone1=response.xpath("//input[@id='telephone']/following-sibling::small/text()").get()
            if phone1:
                phone1=phone1.split(":")[-1].strip()
                item_loader.add_value("landlord_phone",phone1)





        yield item_loader.load_item()  
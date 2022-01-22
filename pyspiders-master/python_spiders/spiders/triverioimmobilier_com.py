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
    name = 'triverioimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Triverioimmobilier_PySpider_france'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"1", "house":"2"}

        for key, value in kwargs.items():
            formdata = {
                "nature": "2",
                "type": value,
                "price": "",
                "city": "",
            }
            yield FormRequest("https://triverioimmobilier.com/fr/locations",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key})


    # 1. FOLLOWING 
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//ul[@class='_list listing']//li[@class='property initial']//a//@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
            seen = True
        border=response.xpath("//a[@rel='next']/text()").get()
        if border:
            if page == 2 or seen:
                if page<int(border)+1:
                    p_url = f"https://triverioimmobilier.com/fr/locations?page={page}/"
                    yield Request( 
                        p_url,
                        callback=self.parse,
                        dont_filter=True,
                        meta={"property_type":response.meta["property_type"], "page":page+1}
                    )
        



    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title)

        external_id = response.xpath("//script[@type='application/ld+json']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('"sku": "')[1].split('"')[0])

        room_count = "".join(response.xpath("//div[@class='details']/ul/li[contains(.,'Pièce')]/text()").getall())
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].split("pièce")[0].strip())
        else:
            room_count =response.xpath("//ul//li[contains(@class,'module-breadcrumb-tab')]//h2//a//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(",")[1].split("pièce")[0].strip())

        square =response.xpath("//ul//li[contains(.,'Jardin ')]//span//text()").get()
        if square:
            item_loader.add_value("square_meters", square.split("m²")[0])
        else:
            square =response.xpath("//ul//li[contains(.,'Loi Boutin')]//span//text()").get()
            if square:
                item_loader.add_value("square_meters", square.split("m²")[0])
                
        utilities = response.xpath("//div[@class='module module-33149 module-property-info property-info-template-7 ']//ul//li[contains(.,'Provision sur charges')]//span//text()").get()
        if utilities :
            item_loader.add_value("utilities", utilities.split("€ / Mois")[0])

        rent = response.xpath("//h2[@class='price']//text()").get()
        if " " in rent :
            price = rent.split("€ / Mois (Charges comprises)")[0]
            item_loader.add_value("rent", price.replace(" ",""))
        else:
            price = rent.split("€ / Mois (Charges comprises)")[0]
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")

        deposit = "".join(response.xpath("substring-after(//p[@class='comment']//text()[contains(.,'Dépôt de garantie')],'Dépôt de garantie : ')").getall())
        if deposit:
            p_deposit = deposit.split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("deposit", int(float(p_deposit)))
        else:
            deposit = response.xpath("//p[@id='description']//text()[contains(.,'Dépôt de garantie')]").get()
            if deposit:
                p_deposit = deposit.split("Dépôt de garantie")[1].split("€")[0]
                item_loader.add_value("deposit", p_deposit)
        
        description = " ".join(response.xpath("//p[@class='comment']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.strip())

        floor = " ".join(response.xpath("//div[@class='details']/ul/li[contains(.,'Étage')]/text()").getall()).strip()   
        if floor:
            item_loader.add_value("floor", floor.split(":")[1].split("ème")[0].strip())

        # address = " ".join(response.xpath("//div[@class='title']/h1/text()").getall()).strip()   
        # if address:
        #     item_loader.add_value("address", address.split("-")[1].strip())

        city = response.xpath("//div[@class='module module-33141 module-property-info property-info-template-19 ']//h1//span//text()").get()
        if city:
            item_loader.add_value("city", city)
            item_loader.add_value("address",city)

        images = [ response.urljoin(x) for x in response.xpath("//img[@class='picture ']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("//ul[@class='list-inline list-inline-30']//li[contains(.,'Meublé')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            item_loader.add_value("furnished", False)

        available_date=response.xpath("//div[@class='details']/ul/li[contains(.,'Available')]/text()[.!='Libre']").get()

        if available_date:
            date2 =  available_date.split(":")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            if date_parsed.year <= 2016: return
            item_loader.add_value("available_date", date3)

        services = " ".join(response.xpath("//div[@class='details']/h2[contains(.,'Services')]/following-sibling::ul/li/text()").getall()).strip()   
        if services:
            if "Ascenseur" in services :
                item_loader.add_value("elevator", True)
            if "Meublé" in services:
                item_loader.add_value("furnished", True)
            if "Lave-vaisselle" in services:
                item_loader.add_value("dishwasher", True)
            if "Lave-linge" in services:
                item_loader.add_value("washing_machine", True)
            if "Piscine" in services:
                item_loader.add_value("swimming_pool", True)

        terrace = " ".join(response.xpath("//div[@class='details last']/ul/li[contains(.,'Terrasse')]/text()").getall()).strip()   
        if terrace:
            item_loader.add_value("terrace", True)

        parking = " ".join(response.xpath("//div[@class='details last']/ul/li[contains(.,'Garage')]/text()").getall()).strip()   
        if parking:
            item_loader.add_value("parking", True)

        energy_label = " ".join(response.xpath("substring-before(substring-after(//div[@class='diagnostic']/img/@src,'value/'),'/')").getall()).strip()   
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(int(float(energy_label))))

        item_loader.add_value("landlord_phone", "+33 (0)4 92 18 04 00")
        item_loader.add_value("landlord_name", "Triverio Immobilier")
        item_loader.add_value("landlord_email", "contact@triverioimmobilier.com")

        if not item_loader.get_collected_values("parking"):
            if response.xpath("//li[contains(.,'Abri de voiture')]").get(): item_loader.add_value("parking", True)

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


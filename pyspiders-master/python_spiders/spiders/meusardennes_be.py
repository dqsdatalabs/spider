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
    name = 'meusardennes_be'
    execution_type='testing'
    country='belgium'
    locale='fr' 
    external_source="Meusardennes_PySpider_belgium_fr"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.meusardennes.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20true,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%202%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.meusardennes.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20true,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D",
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

        for item in response.xpath("//a[@class='estate-card']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        title = "".join(response.xpath("//h1[contains(@class,'intro__text')]//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        category = response.xpath("//tr/th[contains(.,'Catégorie')]/following-sibling::td//text()").get()
        if "Studio" in category:
            item_loader.add_value("property_type","studio")
            item_loader.add_value("room_count","1")
        elif "Chambre étudiant" in category:
            item_loader.add_value("property_type","room")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        rent = "".join(response.xpath("//h1[contains(.,'€')]//text()").getall())
        if rent:
            price = rent.split("€")[0].split("-")[-1].strip().replace(' ', '').replace("\xa0","")
            item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//tr/th[contains(.,'chambre')]/following-sibling::td//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        elif "Chambre étudiant" in category:
            item_loader.add_value("room_count", "1")
            
        bathroom_count = response.xpath("//tr/th[contains(.,'salle')]/following-sibling::td//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        square_meters = response.xpath("//tr/th[contains(.,'Surface')]/following-sibling::td//text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        desc = ",".join(response.xpath("//div/h2[contains(.,'desc') or contains(.,'Desc')]/parent::div/p//text()").getall())
        if desc:
            desc = desc.replace("\u20ac"," ")
            item_loader.add_value("description", desc.strip())
        
        if "garantie locative :" in desc.lower():
            deposit = desc.lower().split("garantie locative :")[1].replace(",","").strip().split(" ")[0]
            item_loader.add_value("deposit", deposit)
        
        if "\u00e9tage" in desc:
            floor = desc.split("\u00e9tage")[0].strip().split(" ")[-1].replace("ème","").replace("er","").replace("e","")
            if floor.isdigit():
                item_loader.add_value("floor", floor)
        
        if "lave vaisselle" in desc.lower():
            item_loader.add_value("dishwasher", True)
        
        washing = "".join(response.xpath(
            "//div/h2[contains(.,'desc') or contains(.,'Desc')]/parent::div/p//text()[contains(.,'machine à laver')]"
            ).getall())
        if washing:
            item_loader.add_value("washing_machine", True)
        
        furnished = response.xpath(
            "//div/h2[contains(.,'desc') or contains(.,'Desc')]/parent::div/p//text()[contains(.,' meuble') or contains(.,' Meuble') or contains(.,' meublé') ]"
            ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        address = response.xpath("//h3[contains(.,'Adresse')]/following-sibling::p//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(" ")[-1])
            item_loader.add_value("zipcode", address.split(" ")[-2])
        else:
            item_loader.add_value("address", desc.strip().split(".")[0])
        
        energy_label = response.xpath("//span/img[contains(@class,'peb')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("peb-")[1].split(".")[0].upper())
        
        if "Disponible" in desc:
            available_date = desc.split("Disponible")[1].split(",")[0].strip()
            if "médiatement" not in available_date:
                available_d = available_date.replace("le ","").replace(".","").replace(":","").strip()
                if available_d:
                    date_parse = dateparser.parse(available_d, date_formats=["%d/%m/%Y"])
                    if date_parse:
                        date2 = date_parse.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
                
        external_id = response.xpath("//tr/th[contains(.,'Référence')]/following-sibling::td//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        terrace = "".join(response.xpath("//tr/th[contains(.,'Terrasse')]/following-sibling::td//text()").getall())
        if terrace and "Oui" in terrace:
            item_loader.add_value("terrace", True)
        
        parking = "".join(response.xpath("//tr/th[contains(.,'Parking')]/following-sibling::td//text()").getall())
        garage = "".join(response.xpath("//tr/th[contains(.,'Garag')]/following-sibling::td//text()").getall())
        if parking or garage:
            if "Oui" in parking or "Oui" in garage:
                item_loader.add_value("parking", True)
            elif "Non" in parking or "Non" in garage:
                item_loader.add_value("parking", False)
        
        furnished = "".join(response.xpath("//tr/th[contains(.,'Meublé')]/following-sibling::td//text()").getall())
        if furnished and "Oui" in furnished:
            item_loader.add_value("furnished", True)
        elif furnished and "Non" in furnished:
            item_loader.add_value("furnished", False)
        
        charges = response.xpath("//tr/th[contains(.,'(montant)')]/following-sibling::td//text()").get()
        if charges:
            item_loader.add_value("utilities", charges)
        
        elevator = "".join(response.xpath("//tr/th[contains(.,'Ascenseur')]/following-sibling::td//text()").getall())
        if elevator and "Oui" in elevator:
            item_loader.add_value("elevator", True)
        elif elevator and "Non" in elevator:
            item_loader.add_value("elevator", False)
        
        images = [x for x in response.xpath("//div[@class='owl-estate-photo']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "MEUSARDENNES")
        phone = response.xpath("//a/@href[contains(.,'tel')]").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split(":")[1].strip())
        item_loader.add_value("landlord_email", "info@meusardennes.be")
        
        
        yield item_loader.load_item()
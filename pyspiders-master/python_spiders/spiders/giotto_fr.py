# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector 
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy
class MySpider(Spider):
    name = 'giotto_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    # url = "https://www.giotto.fr/fr/recherche"
    # headers = {
    #     "authority": "www.giotto.fr",
    #     "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    #     "accept-encoding": "gzip, deflate, br",
    #     "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    #     "cache-control": "max-age=0",
    #     "content-type": "application/x-www-form-urlencoded",
    #     "cookie": "device_view=full; _gcl_au=1.1.1970360824.1631686052; _ga=GA1.2.799477723.1631686054; EU_COOKIE_LAW_CONSENT=true; PHPSESSID=3mtmlo87ptgvna1m73ii6h5mtl; _gid=GA1.2.1825468730.1631786806",
    #     "origin": "https://www.giotto.fr",
    #     "referer": "https://www.giotto.fr/fr/recherche",
    #     "sec-ch-ua-mobile": "?0",
    #     "sec-fetch-dest": "document",
    #     "sec-fetch-mode": "navigate",
    #     "sec-fetch-site": "ame-origin",
    #     "sec-fetch-user": "?1",
    #     "upgrade-insecure-requests": "1",
    #     "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
    # }

    # def start_requests(self):
    #     start_urls = [
    #         {
    #             "formdata" : {
    #                 'search-form-20453[search][category]': 'Location|2',
    #                 'search-form-20453[search][type]': 'Appartement|1',
    #                 'search-form-20453[search][price_min]':"", 
    #                 'search-form-20453[search][price_max]':"", 
    #                 'search-form-20453[search][sector]':"", 
    #                 'search-form-20453[search][reference]':"", 
    #                 'search-form-20453[submit]':"", 
    #                 'search-form-20453[search][order]':"", 
    #                 },
    #             "property_type" : "apartment",
    #         },
    #         {
    #             "formdata" : {
    #                 'search-form-20453[search][category]': 'Location|2',
    #                 'search-form-20453[search][type]': 'Maison|2',
    #                 'search-form-20453[search][price_min]':"", 
    #                 'search-form-20453[search][price_max]':"", 
    #                 'search-form-20453[search][sector]':"", 
    #                 'search-form-20453[search][reference]':"", 
    #                 'search-form-20453[submit]':"", 
    #                 'search-form-20453[search][order]':"", 
    #                 },
    #             "property_type" : "house"
    #         },
    #     ]
    def start_requests(self):
        url = "https://www.giotto.fr/fr/recherche"
        yield Request(url,
                        callback=self.parse,)


    def parse(self, response):

        for item in response.xpath("//li[@class='property initial']//a[2]//@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item,)
        
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), 
                         callback=self.parse,
                         dont_filter=True)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "locations" in response.url:
    
            item_loader.add_value("external_link", response.url)
            property_type=" ".join(response.xpath("//li[@class='module-breadcrumb-tab']//a/text()").getall())
            if property_type:
                if "appartement" in property_type.lower():
                    item_loader.add_value("property_type","apartment")
                if "maison" in property_type.lower():
                    item_loader.add_value("property_type","house")

            item_loader.add_value("external_source", "Giotto_PySpider_france")     
            title = response.xpath("//div[@class='zone zone-full-width ']//h1/text()").get()
            if title:
                item_loader.add_value("title", title.strip()) 
                if "meublé" in title or "meuble" in title: 
            
                    item_loader.add_value("furnished", True) 

            item_loader.add_xpath("external_id", "//div[contains(@class,'module-cluster')]//li[text()='Référence ']/span/text()")
            room_count = response.xpath("//li[@class='module-breadcrumb-tab']/h2/a").get()
            if room_count:
                room_count = room_count.split("pièce")[0].split(",")[-1].strip()
                item_loader.add_value("room_count", room_count)

            bathroom_count = response.xpath("//div[contains(@class,'module-property-info')]//li[contains(.,'Salle de bain')]/text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.split("Salle")[0])
            address = response.xpath("//div[@class='zone zone-full-width ']//h1/span/text()").get()
            if address:
                item_loader.add_value("address", address)
                item_loader.add_value("city", address.strip())
            floor = response.xpath("//div[contains(@class,'module-cluster')]//li[text()='Étage ']/span/text()").get()
            if floor:
                item_loader.add_value("floor", floor.split("étage")[0].strip())
        
            furnished = response.xpath("//li[contains(.,'Meublé') or contains(.,'Meubl')]/text()").get()
            if furnished:
                item_loader.add_value("furnished", True)
            elevator = response.xpath("//li[.='Ascenseur']/text()").get()
            if elevator:
                item_loader.add_value("elevator", True)
            parking = response.xpath("//div[contains(@class,'module-property-info')]//li[contains(.,'Parking') or contains(.,' Garage')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
            terrace = response.xpath("//div[contains(@class,'module-property-info')]//li[contains(.,'Terrasse')]/text()").get()
            if terrace:
                item_loader.add_value("terrace", True)
            balcony = response.xpath("//div[contains(@class,'module-property-info')]//li[contains(.,'Balcon')]/text()").get()
            if balcony:
                item_loader.add_value("balcony", True)
            swimming_pool = response.xpath("//li[.='Piscine']/text()").get()
            if swimming_pool:
                item_loader.add_value("swimming_pool", True)
            square_meters = response.xpath("//div[contains(@class,'module-cluster')]//li[text()='Surface ']/span/text()").get()
            if square_meters:
                item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].replace(",",".").strip())))
        
            description = " ".join(response.xpath("//p[@id='description']//text()").getall()) 
            if description:
                item_loader.add_value("description", description.strip())

            images = [x for x in response.xpath("//div[contains(@class,'slider module-cluster')]//div[@class='slider']//a/@href").getall()]
            if images:
                item_loader.add_value("images", images)
            rent = response.xpath("//p[@class='price']/text()").get()
            if rent:
                rent = rent.split("€")[0].replace(" ","")
                item_loader.add_value("rent", rent)
            utilities = response.xpath("//li[contains(.,'Provision sur charges')]/span/text()").get()
            if utilities:
                item_loader.add_value("utilities", utilities)
            deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/span/text()").get()
            if deposit:
                item_loader.add_value("deposit", deposit.strip().replace("\u202f","").replace(" ",""))
            # landlord_name = response.xpath("//div[@class='info']/h3/a/text()").get()
            # if landlord_name:
            #     item_loader.add_value("landlord_name", landlord_name.strip())
            item_loader.add_value("landlord_name","GIOTTO")
            landlord_phone = response.xpath("//div[@class='info']//span[@class='phone']/a/text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
            landlord_email = response.xpath("//div[@class='info']//span[@class='email']/a/text()").get()
            if landlord_email:
                item_loader.add_value("landlord_email", landlord_email.strip())
            item_loader.add_value("currency","EUR")


            yield item_loader.load_item()
# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request 
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re  
import dateparser  

class MySpider(Spider):
    name = "cabinet069_be"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source='Cabinet069_PySpider_belgium_fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.cabinet069.be/Chercher-bien-accueil--L--resultat?pagin=0&regionS=&communeS=&type=Appartement&prixmaxS=&chambreS=&keyword=&viager=&listeLots=",
                "property_type" : "apartment"
            },
            {
                "url" : "http://www.cabinet069.be/Chercher-bien-accueil--L--resultat?pagin=0&regionS=&communeS=&type=maison/villa&prixmaxS=&chambreS=&keyword=&viager=&listeLots=",
                "property_type" : "house"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 1) 

        seen = False
        for item in response.xpath(
            "//div[contains(@class,'portfolio-wrapper')]/div[contains(@class,'portfolio-item')]//a[@class='portfolio-link']/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(
                follow_url, callback=self.populate_item, meta={"property_type": response.meta.get("property_type")}
            )
            seen = True 

        if page == 1 or seen:
            if "Appartement" in response.url:
                url = f"http://www.cabinet069.be/Chercher-bien-accueil--L--resultat?pagin={page}&regionS=&communeS=&type=Appartement&prixmaxS=&chambreS=&keyword=&viager=&listeLots="
            elif "maison" in response.url:
                url = f"http://www.cabinet069.be/Chercher-bien-accueil--L--resultat?pagin={page}&regionS=&communeS=&type=maison/villa&prixmaxS=&chambreS=&keyword=&viager=&listeLots="
           
            yield Request(url, callback=self.parse, meta={"page": page + 1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Cabinet069_PySpider_" + self.country + "_" + self.locale)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        square_meters = response.xpath(
            "//div[@class='row fontIcon']/div[contains(text(),'habitation')]/text()"
        ).get()
        
        room = response.xpath(
            "//div[contains(@class,'row fontIcon')]/div[contains(.,'chambre')]/text()"
        ).extract_first()
        if room:
            item_loader.add_value("room_count", room.split(" ")[0]) 
        elif response.xpath("//h1[contains(.,'Studio') or contains(.,'studio')]/text()").get():
            item_loader.add_value("room_count", "1")
        roomcheck=item_loader.get_output_value("room_count")
        if not roomcheck:
            room=response.xpath("//title//text()").get()
            room=room.split("chambre")[0].split(",")[-1]
            room=re.findall("\d+",room)
            item_loader.add_value("room_count",room)

        desc="".join(response.xpath("//div[contains(@class,'col-lg-6')]/p//text()").extract()).split(":")[0]
        desc=desc.replace("Caract\u00e9ristiques","")
        item_loader.add_value("description", desc)
        desc1="  ".join(response.xpath("//div[@data-bind='html: currentShortDescription']//text()").getall()).split(":")[0]
        desc1=desc1.replace("Caract\u00e9ristiques","")
        if desc1:
            item_loader.add_value("description", desc1)
            
        # desc = "".join(
        #     response.xpath("//div[contains(@class,'col-lg-6')]/p//text()").extract()).strip()
        # item_loader.add_value("description", desc)

        rent = response.xpath("//strong/span/text()").get()
        if rent:
            rent = rent.split("€")[0]
            rent1=rent.replace(" ", "")
            item_loader.add_value("rent", rent1)
        item_loader.add_value("currency", "EUR")
 
        ref = response.xpath("//h2/text()").get()
        if ref: 
            item_loader.add_value("external_id", ref.split("REF")[1])
        
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'owl-carousel')]//img/@src").extract()]
        item_loader.add_value("images", images)

        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("habitation")[1].split("m")[0])
        else:
            square_meters = response.xpath("//div[contains(@class,'col-lg-6')]/p//text()[contains(.,'m²')]").re(r'(\d+\s*)m²')
            if square_meters:
                square_meters = sum(int(s.strip()) for s in square_meters)
                item_loader.add_value("square_meters", square_meters)
 
        address = response.xpath(
            "normalize-space(//div[@class='col-12']/p/text())"
        ).extract_first()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", split_address(address, "city"))
        
        furnished = response.xpath(
            "//div[@class='container-fluid text-center']//div[.='équipée']"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        washing_machine=response.xpath(
            "//div[contains(@class,'col-lg-6')]/p//text()[contains(.,'machine à laver')]"
            ).get()
        if washing_machine:
            item_loader.add_value("washing_machine",True)
        
        if "lave-vaisselle" in desc.lower():
            item_loader.add_value("dishwasher", True)
         
        energy_label=response.xpath("//div/img[contains(@src,'peb')]/@src").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split(".")[0].split("/")[1])
        
        phone = response.xpath(
            '//div[@class="gallery-img"]//a[contains(@href, "tel:")]/@href'
        ).get()
        if phone: 
            item_loader.add_value("landlord_phone", phone.replace("tel:", ""))


        item_loader.add_value("landlord_email","info@cabinet069.be")
        item_loader.add_value("landlord_name", "AGENCE IMMOBILIERE 069") 
        
        utilities = response.xpath('//p[contains(., "Charges")]//text()').re_first(r'\d+€')
        if utilities:
            item_loader.add_value("utilities", utilities.replace('€', '').strip())
        else:
            utilities = response.xpath('//div[@class="col-12 col-lg-6"]/p//span/text()').extract()
            utilities = [*filter(lambda u: '€' in u, utilities)]
            if utilities:
                item_loader.add_value("utilities", re.sub(r'\D', '', utilities[0]))
        
        parking = response.xpath('//div[@class="col ficheIcon"]/text()').getall()
        if parking:
            for i in parking:
                if "garage" in i:
                    item_loader.add_value("parking", True)
         
        available_date = response.xpath("//h1/text()").re(r'\d\w{2}\s\w+\s\d{4}')
        if available_date:
            date_parsed = dateparser.parse(available_date[0], date_formats=["%m-%d-%Y"], locales=['fr'])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)
        yield item_loader.load_item()


def split_address(address, get):
    city = address.split(" ")[-1]
    return city
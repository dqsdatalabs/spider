# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'lesclesdelimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Lesclesdelimmobilier_PySpider_france_fr'
    custom_settings = {
        "HTTPCACHE_ENABLED": False
    }


    def start_requests(self):

        start_urls = [
            # {
            #     "url" : "https://lesclefsdelimmobilier.fr/immobilier-louer?typeBien%5B0%5D=1&page=1",
            #     "property_type" : "house"
            # },
            {
                "url" : "https://lesclefsdelimmobilier.fr/immobilier-louer?typeBien%5B0%5D=2&page=1&maxPerPage=10&statutBien%5B0%5D=7&statutBien%5B1%5D=8&sort=updated%7CDESC",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        token = response.xpath("//input[@id='filtres__token']//@value").get()
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://lesclefsdelimmobilier.fr",
            "referer": "https://lesclefsdelimmobilier.fr/immobilier-louer?typeBien%5B0%5D=2&page=1&sort=numeroMandat%7CDESC",
            "sec-ch-ua": '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
            "sec-ch-ua-mobile": "?0",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
            }
       
        formdata = {
            'filtres[typeBien][]':'2',
            'filtres[budgetMin]':'',
            'filtres[budgetMax]':'', 
            'filtres[surfaceMin]':'',
            'filtres[surfaceMax]':'',
            'filtres[reference]':'',
            'filtres[page]':'1',
            'filtres[maxPerPage]':'10',
            'filtres[routeName]':'page_louer',
            'filtres[typeOffre]':'location', 
            'filtres[statutBien][]':'7',
            'filtres[statutBien][]':'8',
            'filtres[sort]':'updated|DESC',
            'filtres[_token]':f'{token}',
        } 

        url = "https://lesclefsdelimmobilier.fr/get-annonces"

        yield FormRequest(
            url,
            formdata=formdata,
            headers=headers,
            callback = self.parse_listing,
            dont_filter=True,
            meta={"property_type" : response.meta.get("property_type")}
            )
        
    def parse_listing(self,response):
        sel = Selector(text=json.loads(response.body)["html"], type="html")
        for item in sel.xpath("//div[contains(@class,'card-annonce')]/a/@href").getall():
            yield Request(item, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = "".join(response.xpath("//h2//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        external_id = response.xpath("//div[@class='referenceBien']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(" ")[0])
        
        city = response.xpath("//dt[contains(.,'Ville')]/following-sibling::dd/text()").get()
        if city:
            item_loader.add_value("address", city)
            item_loader.add_value("city", city)
        
        zipcode = response.xpath("//dt[contains(.,'Code')]/following-sibling::dd/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
        square_meters = response.xpath("//i[contains(@class,'house-size')]/following-sibling::span/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        rent = response.xpath("//div[@class='tarification']//text()").get()
        if rent:
            rent = rent.split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//li[@class='col-3 text-center']/span[contains(text(),'pièce')]/text()").get()
        if room_count:
            room_count = room_count.split()[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//dt[contains(.,'Salle')]/following-sibling::dd/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        floor = response.xpath("//dt[contains(.,'Etage')]/following-sibling::dd/text()").get()
        if floor:
            item_loader.add_value("floor", floor)        
        
        utilities = response.xpath("//dt[contains(.,'Charge')]/following-sibling::dd/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(" ")[0])
        
        balcony = response.xpath("//dt[contains(.,'Balcon')]/following-sibling::dd/text()[.='Oui']").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//dt[contains(.,'Terrasse')]/following-sibling::dd/text()[.='Oui']").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//dt[contains(.,'Parking')]/following-sibling::dd/text()[.='Oui']").get()
        if parking:
            item_loader.add_value("parking", True)
        
        description = "".join(response.xpath("//p[@class='text-big']//text()").extract())
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//ul[@id='imageGallery']//@src").getall()]
        item_loader.add_value("images", images)
        
        energy_label = response.xpath("//div[@class='energie']/div[contains(@class,'energie-item') and contains(@class,'active')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        landlord_name = response.xpath("//span[@class='titre']/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Les clefs de l'immobilier")
        
        landlord_phone = response.xpath("//a[contains(@class,'btn-tel')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            item_loader.add_value("landlord_phone", "03 26 40 39 39")
        
        yield item_loader.load_item()
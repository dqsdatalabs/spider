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
import datetime
from datetime import date

 
class MySpider(Spider):
    name = 'vivastreet_be'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source="Vivastreet_PySpider_belgium"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://search.vivastreet.be/annonces-location-appartement-meuble/be?lb=new&search=1&start_field=1&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D=&keywords=&cat_1=137&geosearch_text=&searchGeoId=0&sp_housing_monthly_rent%5Bstart%5D=&sp_housing_monthly_rent%5Bend%5D=&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://search.vivastreet.be/location-maison/be?lb=new&search=1&start_field=1&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D=&keywords=&cat_1=279&geosearch_text=&searchGeoId=0&sp_housing_monthly_rent%5Bstart%5D=&sp_housing_monthly_rent%5Bend%5D=&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D=",
                    "https://search.vivastreet.be/maison-meublee/be?lb=new&search=1&start_field=1&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D=&keywords=&cat_1=281&geosearch_text=&searchGeoId=0&sp_housing_monthly_rent%5Bstart%5D=&sp_housing_monthly_rent%5Bend%5D=&sp_housing_nb_bedrs%5Bstart%5D=&sp_housing_nb_bedrs%5Bend%5D=&sp_housing_sq_ft%5Bstart%5D=&sp_housing_sq_ft%5Bend%5D="
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
        for item in response.xpath("//a[@class='clad__ad_link']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(.,'Suivante')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]}
            )    
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        studio = "".join(response.xpath("//table//tr[td[.=' Nbre de chambres ']]/td[2]/text()").extract())
        if "studio" in studio.lower().strip():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        external_id = response.xpath("//span[contains(.,'Annonce N')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//td[contains(.,'Ville/Code postal')]/following-sibling::td/div/text()[last()]").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split('-')[-1].strip())
            item_loader.add_value("city", address.split('-')[0].strip())
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(text(),'Description')]/following-sibling::div//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//td[contains(.,'Surface')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].strip())

        room_count = response.xpath("//td[contains(.,'Nbre de chambres')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())

        rent = response.xpath("//div[@id='title_price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')
        availabledate=response.xpath("//span[@id='posted-by-msg']//span[last()]/text()").get()
        if availabledate:
            available=availabledate.split("le")[-1].replace("/","-").strip()
            if not "now" in available.lower():
                date_parsed = dateparser.parse(available,date_formats=["%d-%m-%Y"])
                if date_parsed: 

                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        datecheck=item_loader.get_output_value("available_date")
        if datecheck:
            datetimeobj=datetime.datetime.strptime(datecheck,'%Y-%m-%d')
            today=datetime.datetime.now()
            date1=date(today.year,today.month,today.day)
            date2=date(datetimeobj.year,datetimeobj.month,datetimeobj.day)
            abs=int((date1-date2).days)
            if abs>60:
                return

            
        
        deposit = response.xpath("//text()[contains(.,'Garantie locative')]").get()
        if deposit:
            if '€' in deposit:
                item_loader.add_value("deposit", deposit.split('€')[0].strip().split(' ')[-1].strip())
            elif 'mois de loyer' in deposit:
                multiple = int("".join(filter(str.isnumeric, deposit.strip())))
                item_loader.add_value("deposit", str(multiple * int(rent)))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='vs_photo_viewer_container']//img/@src").getall()]
        images +=  [response.urljoin(x) for x in response.xpath("//div[@id='vs_photo_viewer_container']//img/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        latitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"geo_latitude":')[1].split(',')[0].strip().strip('"').strip())
            item_loader.add_value("longitude", latitude.split('"geo_longitude":')[1].split(',')[0].strip().strip('"').strip())
        
        energy_label = response.xpath("//text()[contains(.,'PEB') and contains(.,'Classe')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('Classe')[-1].strip().upper())

        utilities = response.xpath("//text()[contains(.,'Charges') and contains(.,':')]").get()
        if utilities:
            if '€' in utilities:
                item_loader.add_value("utilities", utilities.split('€')[0].strip().split(' ')[-1].strip())
        
        parking = response.xpath("//p[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//p[contains(.,'Balcon') or contains(.,'balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//p[contains(.,'Ascenseur') or contains(.,'ascenseur')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//p[contains(.,'Terrasse') or contains(.,'terrasse')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        swimming_pool = response.xpath("//p[contains(.,'Piscine') or contains(.,'piscine')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        washing_machine = response.xpath("//p[contains(.,'Laver') or contains(.,'laver')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)

        item_loader.add_value("landlord_name", "Vivastreet")

        landlord_phone = response.xpath("//span[@id='phone-button-dt']/@data-phone-number").get()
        if landlord_phone: item_loader.add_value("landlord_phone", landlord_phone.strip())
        phonecheck=item_loader.get_output_value("landlord_phone")
        if not phonecheck:
            item_loader.add_value("landlord_phone","02 808 35 16")
              
        yield item_loader.load_item() 
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
    name = 'beneatchauvel_com'
    start_urls = ['https://www.beneat-chauvel.com/location']  # LEVEL 1
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {"url": "https://www.beneat-chauvel.com/location/appartement", "property_type": "apartment"},
	        {"url": "https://www.beneat-chauvel.com/location/maison", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='row photo']"):
            url = item.xpath("./div[1]/a/@href").get()            
            follow_url = response.urljoin(url)
            city = item.xpath(".//div[contains(@class,'localisation')]/p/text()").extract_first()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),"city":city})
        
        pagination = response.xpath("//div[@class='immobilier-search-row']//ul[@class='pager']/li[contains(@class,'pager-next')]/a/@href").extract_first()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Beneatchauvel_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_css("title", "h1")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.xpath("normalize-space(//strong[contains(.,'Référence')]/following-sibling::text())").get())
   
        description = "".join(response.xpath("//div[@class='body']/p/text()").extract())
        if description:
            item_loader.add_value("description", description)
            if "meublé" in description.lower():
                item_loader.add_value("furnished",True)

            if "parking" in description.lower() or "garage" in description.lower():
                item_loader.add_value("parking", True)
            
            if "terrasse" in description.lower():
                item_loader.add_value("terrace", True)

        address = response.xpath("normalize-space(//strong[contains(.,'Secteur')]/following-sibling::text())").get()
        if address:
            item_loader.add_value("address",address.strip())
        city_zipcode = response.meta.get('city')
        if city_zipcode:
            if "(" in city_zipcode:
                zipcode = city_zipcode.split("(")[1].split(")")[0]
                city = city_zipcode.split("(")[0]
                item_loader.add_value("city", city.strip())            
                item_loader.add_value("zipcode",zipcode.strip())
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = response.xpath("normalize-space(//strong[contains(.,'habitable')]/following-sibling::text())").get()
        if square_meters:
            if square_meters != "":
                square_meters = square_meters.split(" ")[0]
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("normalize-space(//strong[contains(.,'Nombre de chambre')]/following-sibling::text())").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("normalize-space(//strong[contains(.,'Nombre de pièces')]/following-sibling::text())").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("normalize-space(//strong[contains(.,'salles de bain')]/following-sibling::text())").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        elif not bathroom_count:
            bathroom_count = response.xpath("normalize-space(//strong[contains(.,'Nombre de salles ') and contains(.,'eau') ]/following-sibling::text())").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count)

        available_date = response.xpath("normalize-space(//strong[contains(.,'Disponibilité')]/following-sibling::text())").get()
        if available_date and available_date.replace(" ","").isalpha() != True:
            try:
                available_date = available_date.split(" ")[1]
            except:
                pass
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[./h2[.='Photos']]/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        price = response.xpath("//strong[contains(.,'Loyer')]/following-sibling::text()").get()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))     
        
        deposit = response.xpath("normalize-space(//strong[contains(.,'Dépôt')]/following-sibling::text())").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip().replace(" ",""))
        utilities = response.xpath("//strong[contains(.,'Provision sur charges')]/following-sibling::text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("€")[0].strip().replace(" ",""))
        elevator = response.xpath("//strong[contains(.,'Ascenseur')]/following-sibling::text()").get()

        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)

        energy_label = response.xpath("//div[@class='col-xs-6 col-sm-6 col-md-3 col-md-offset-3 col-lg-3 col-lg-offset-3 dpe']/img/@src").get()
        if energy_label and "none" not in energy_label:
            item_loader.add_value("energy_label", energy_label.split("/")[-1].split("-")[-1].split(".")[0].upper())

        landlord_name = response.xpath("//p[./strong[contains(.,'Agence')]]/strong/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name.split(":")[0].strip())
        
        landlord_phone = response.xpath("normalize-space(//p[./strong[contains(.,'Agence')]]/text()[3])").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip("\xa0"))
        
        landlord_email = response.xpath("//p[./strong[contains(.,'Agence')]]/span/a/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)

        latlng = response.xpath("//div[@id='gmap']/@data-pos").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split(",")[0].strip().replace("[",""))
            item_loader.add_value("longitude", latlng.split(",")[1].strip().replace("]",""))
        
        yield item_loader.load_item()
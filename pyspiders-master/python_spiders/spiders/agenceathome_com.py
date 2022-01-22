# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'agenceathome_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        url = "http://www.agenceathome.com/index.php/index_v2/search/french"
        property_types = [
            {
                "formdata" : {
                    'type_transaction': 'Louer',
                    'type_bien': '1',
                    'NBRE_CHAMBRES': 'on',
                    },
                "property_type" : "apartment"
            },
        ]
        for item in property_types:
            yield FormRequest(url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"]})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='img-link']/following-sibling::a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
             
        item_loader.add_value("external_link", response.url) 
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Agenceathome_PySpider_france")
        title = " ".join(response.xpath("//h1/span/text()").getall())
        if title:
            item_loader.add_value("title", title.replace("\n","").strip())
            
        external_id = response.xpath("//h3/text()[contains(.,'Référence :')]").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[-1])
       
        zipcode = response.xpath("//div[@class='col-md-3']//div[.='Code postal']/following-sibling::div[1]/strong/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        address1 = response.xpath("//div[@class='col-md-3']//div[.='Secteur']/following-sibling::div[1]/strong/text()").get()
        city = response.xpath("//div[@class='col-md-3']//div[.='Ville']/following-sibling::div[1]/strong/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
            if address1 and zipcode:      
                city = city.strip()+ ", " + address1.upper().split("(")[0].strip()+ ", " + zipcode.strip() 
            item_loader.add_value("address", city.strip())
        bathroom_count = response.xpath('//div[.="Salle(s) de bains" or .="Salle(s) d\'eau"]/following-sibling::div[1]/strong/text()').get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        room_count = response.xpath("//div[.='Nombre Chambres']/following-sibling::div[1]/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            item_loader.add_xpath("room_count", "//div[.='Nombre pièces']/following-sibling::div[1]/strong/text()")
      
        description = "".join(response.xpath("//div[@id='commentaires']/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        square_meters = response.xpath("//div[.='Surface']/following-sibling::div[1]/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.split("m")[0].strip())))
        item_loader.add_xpath("floor", "//div[.='Etage(s) du bien']/following-sibling::div[1]/strong/text()")
        item_loader.add_xpath("rent_string", "//div[.='Loyer']/following-sibling::div[1]/strong/text()")
        item_loader.add_xpath("deposit", "//div[.='Dépôt de Garantie']/following-sibling::div[1]/strong/text()")
        item_loader.add_xpath("utilities", "//div[.='Provision sur charge']/following-sibling::div[1]/strong/text()")
        # latitude_longitude = response.xpath("//script[contains(.,'center: {lat: ')]//text()").get()
        # if latitude_longitude:
        #     latitude = latitude_longitude.split('center: {lat: ')[1].split(',')[0]
        #     longitude = latitude_longitude.split('center: {lat: ')[1].split('lng: ')[1].split('}')[0].strip()      
        #     item_loader.add_value("longitude", longitude)
        #     item_loader.add_value("latitude", latitude)
     
        images = [response.urljoin(x) for x in response.xpath("//div[contains(@class,'slider slider-for')]/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)  
            
        item_loader.add_value("landlord_name", "At home immobilier")
        item_loader.add_value("landlord_phone", "01 42 09 45 46")
        item_loader.add_value("landlord_email", "contact@agenceathome.com")

        energy_label = response.xpath("substring-before(//div[.='Conso Energ']/following-sibling::div[1]/strong/text(),' /')").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
        balcony = response.xpath("//h1//text()[contains(.,'Balcon')]").get()
        if balcony:
                item_loader.add_value("balcony", True)
        furnished = response.xpath("//div[.='Meublé']/following-sibling::div[1]/strong/text()").get()
        if furnished:
            if "0" in furnished.upper():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        yield item_loader.load_item()
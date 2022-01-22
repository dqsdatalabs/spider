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
import re

class MySpider(Spider):
    name = 'immosurmesure_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immosurmesure_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {
                "url" : "http://www.immosurmesure.fr/recherche?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher", "property_type" : "apartment"
            },
            {
                "url" : "http://www.immosurmesure.fr/recherche?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&do_search=Rechercher", "property_type" : "house"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse, meta={"property_type": url.get("property_type")})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='result']/div[@class='res_div1']//div[@class='details']/a/@href").extract():
            follow_url = response.urljoin(item)
            
            yield Request(follow_url, callback=self.populate_item, meta={'property_type' : response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        
        rent="".join(response.xpath("//table/tr/td[contains(@itemprop,'price')]//text()[contains(.,'€')]").getall())
        if rent:
            item_loader.add_value("rent_string", rent)
        
        square_meters=response.xpath("//td//div/table/tr/td[contains(.,'Surface')]//following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters.strip())))
        
        room_count=response.xpath("//td//div/table/tr/td[contains(.,'Pièce')]//following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//div[@class='tech_detail']/table//tr/td[contains(.,'Salle de bains') or contains(.,'Salle d')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        city = response.xpath("//table//tr/td[contains(.,'Ville')]/following-sibling::td/span/span/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("//table//tr/td[contains(.,'Ville')]/following-sibling::td/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        address =", ".join(response.xpath("//div[@class='tech_detail']/table//tr/td[contains(.,'Secteur')]/following-sibling::td//text() | //table//tr/td[contains(.,'Ville')]/following-sibling::td/span/span/text()").getall())
        if address:
            item_loader.add_value("address", address)
        utilities =response.xpath("//div[@class='tech_detail']/table//tr/td[contains(.,'Charges')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities)
        elif not utilities:
            utilities =response.xpath("substring-before(substring-after(//div[@class='basic_copro']//text(),' charges'),'€')").get()
            if utilities:
                item_loader.add_value("utilities", utilities)
        deposit = response.xpath("//div[@id='details']//text()[contains(.,'Dépôt de garantie')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip()
            item_loader.add_value("deposit", deposit.replace(" ",""))
        elif not deposit:
            deposit =response.xpath("substring-before(substring-after(//div[@class='basic_copro']//text(),' de garantie'),'€')").get()
            if deposit:
                item_loader.add_value("deposit", deposit.replace(" ",""))
        
        available_date = response.xpath("//div[@id='details']//text()[contains(.,'Libre le')]").get()
        if available_date:
            try:
                available_date = available_date.split("Libre le")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
            
        latitude_longitude = response.xpath("//script[@type='text/javascript']//text()[contains(.,'.setView([')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('.setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('.setView([')[1].split(',')[1].split(']')[0]
            item_loader.add_value("longitude", longitude.strip())
            item_loader.add_value("latitude", latitude.strip())
            
        external_id=response.xpath("//td//div/table/tr/td[contains(.,'Référence')]//following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        desc="".join(response.xpath("//div[@id='details']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[@id='layerslider']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        name=" ".join(response.xpath("//strong[@itemprop='name']//text()").getall())
        item_loader.add_value("landlord_name", name)
        item_loader.add_xpath("landlord_phone","//span[@itemprop='telephone']//text()")
        landlord_email = response.xpath("//script[@type='text/javascript']//text()[contains(.,'email')]").get()
        if landlord_email:
            landlord_email = landlord_email.split("('")[1].split("')")[0].replace("', '","@")
            item_loader.add_value("landlord_email",landlord_email.strip())

        
        floor = response.xpath("//div[@class='tech_detail']/table//tr/td[contains(.,'Étage')]/following-sibling::td/text()[not(contains(.,'RDC'))]").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
            
        balcony = response.xpath(
            "//div[@class='tech_detail']/table//tr/td[contains(.,'Balcon')]/following-sibling::td/text()").get()
        if balcony:
            if "non" in balcony.lower():
                item_loader.add_value("balcony",False)
            else:
                item_loader.add_value("balcony",True)
        terrace = response.xpath(
            "//div[@class='tech_detail']/table//tr/td[contains(.,'Terrasse')]/following-sibling::td/text()").get()
        if terrace:
            if "non" in terrace.lower():
                item_loader.add_value("terrace",False)
            else:
                item_loader.add_value("terrace",True)
        
        furnished = response.xpath(
            "//td//div/table/tr/td[contains(.,'Ameublement')]//following-sibling::td/text()").get()
        if furnished:
            if "non" in furnished.lower():
                item_loader.add_value("furnished",False)
            else:
                item_loader.add_value("furnished",True)
        
        elevator = response.xpath(
            "//div[@class='tech_detail']/table//tr/td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if "non" in elevator.lower():
                item_loader.add_value("elevator",False)
            else:
                item_loader.add_value("elevator",True)
            
        swimming_pool = response.xpath("//td//div/table/tr/td[contains(.,'Piscine')]//following-sibling::td/text()").get()
        if swimming_pool:
            if "non" in swimming_pool.lower():
                item_loader.add_value("swimming_pool",False)
            else:
                item_loader.add_value("swimming_pool",True)
        parking = response.xpath("//div[@class='tech_detail']/table//tr/td[contains(.,'Stationnement')]/following-sibling::td/text()").get()
        if parking:
            if "non" in parking.lower():
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)
        status=None
        status=response.xpath("//div[@id='sold']/text()[contains(.,'Loué')]").get()
        if status==None:
            yield item_loader.load_item()

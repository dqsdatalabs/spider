# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'makelaardijhoekstra_nl'
    execution_type = 'testing' 
    country = 'netherlands'
    locale = 'nl'
    external_source='Makelaardijhoekstra_PySpider_netherlands_nl'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.makelaardijhoekstra.nl/verhuur/?plaats_postcode=&radiuscustom=&prijshuur%5Bmin%5D=&prijshuur%5Bmax%5D="}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        if page > 2:
            data = json.loads(response.body)
            
            for item in data["items"]:
                sel = Selector(text=item, type="html")
                url = sel.xpath("//div[contains(@class,'card card--has-image card--has-gallery')]/div/a/@href").extract_first()
                
                yield Request(url, callback=self.populate_item)
        else:
            seen = False
            for item in response.xpath("//div[contains(@class,'card card--has-image card--has-gallery')]/div/a/@href").extract():
                yield Request(item, callback=self.populate_item)
        
        if page < 13:
            
            headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Origin": "https://www.makelaardijhoekstra.nl",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
            }
    
            data = {
                "__live": "1",
                "__infinite": "item",
                "plaats_postcode": "",
                "radiuscustom": "",
                "type": "",
                "prijshuur[min]": "",
                "prijshuur[max]": "",
                "soortwoning[]": "",
                "soortappartement[]": "",
                "typewoning[]": "",
                "liggingen[]": "",
                "buitenruimtes[]": "",
                "woonOppervlakte": "",
                "slaapkamers": "",
                "perceelOppervlakte": "",
                "bouwperiode": "",
                "woonstijl[]": "",
                "bijzonderheden[]":"",
            }
            
            url = f"https://www.makelaardijhoekstra.nl/verhuur/page/{page}/"
            yield FormRequest(
                url,
                formdata=data,
                headers=headers,
                callback=self.parse,
                meta={"page":page+1}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Makelaardijhoekstra_PySpider_" + self.country + "_" + self.locale)
        status = response.xpath("//div[contains(@class,'woning__status')]/text()").extract_first()
        if status and ("verhuurd" in status.lower() or "optie" in status.lower()):
            return
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("=")[-1])

        property_type = response.xpath("//div[@class='prose less definition clean']/dl/dt[.='Soort object']/following-sibling::dd[1]/text()").get()
        if property_type:
            if "Woonhuis" in property_type:
                parking = response.xpath("//div[contains(@class,'woning__content')]/p//text()[contains(.,'parkeerplaats') or contains(.,'garagebox') or contains(.,'Garagebox') or contains(.,'Parkeerplaats') or contains(.,'schiphuis') or contains(.,'Berging')]").get()
                if not parking:
                    property_type = "house"
                    item_loader.add_value("property_type", property_type)
            elif "Appartement" in property_type:
                property_type = "apartment"
                item_loader.add_value("property_type", property_type)
            

        desc = "".join(response.xpath("//div[contains(@class,'woning__content')]/p/text()").extract())
        item_loader.add_value("description", desc.strip())

        price = response.xpath("//div[@class='prose less definition clean']/dl/dt[.='Huurprijs']/following-sibling::dd[1]/text()").get()
        if price:
            # item_loader.add_value("rent", price.split(" ")[1].split(",-")[0])
            item_loader.add_value("rent_string", price)
        # item_loader.add_value("currency", "EUR")

        
        square = response.xpath(
            "//div[@class='prose less definition clean']/dl/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text()").get()
        if square:
            item_loader.add_value(
                "square_meters", square.split("m")[0]
            )
        room_count = response.xpath(
            "//div[@class='prose less definition clean']/dl/dt[.='Aantal kamers']/following-sibling::dd[1]/text()"
        ).get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        bath_room = response.xpath(
            "//div[@class='prose less definition clean']/dl/dt[.='Aantal badkamers']/following-sibling::dd[1]/text()"
        ).get()
        if bath_room:
            item_loader.add_value("bathroom_count", bath_room)
        street = response.xpath("//div[@class='woning__heading heading bold huge']/h1/text()").get()
        city_zipcode = response.xpath("//div[contains(@class,'fl')]/div[@class='prose clean']/p/text()").get()
        
        if city_zipcode:
            item_loader.add_value("address", street + " " + city_zipcode )
            item_loader.add_value("zipcode", split_address(city_zipcode, "zip"))
            item_loader.add_value("city", split_address(city_zipcode, "city").strip())

        item_loader.add_xpath("floor", "//div[@class='prose less definition clean']/dl/dt[.='Aantal verdiepingen']/following-sibling::dd[1]/text()")

        parking = response.xpath("//div[@class='prose less definition clean']/dl/dt[.='Parkeer faciliteiten' or .='Garage']/following-sibling::dd/text()").get()
        if parking:
            if "geen" in parking or "no" in parking:
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        elevator = response.xpath("//div[@class='prose less definition clean']/dl/dt[.='Lift']/following-sibling::dd/text()").get()
        if elevator:
            if "Ja" in elevator:
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)
        map_coor = response.xpath("//script[@id='wonen-single-js-extra']//text()[contains(.,'longitude') and contains(.,'latitude')]").extract_first()
        if map_coor:
            lat = map_coor.split('latitude":')[1].split(",")[0]
            lng = map_coor.split('longitude":')[1].split(",")[0]
            item_loader.add_value("longitude", lng.strip())
            item_loader.add_value("latitude", lat.strip())

        floor_img = response.xpath("//script[@type='text/javascript']//text()[contains(.,'floorplan')]").extract_first()
        images_floor = []
        if floor_img:
            try:
                image =  floor_img.split("= ")[1].split(";")[0]
                json_l = json.loads(image)
                for key,value in json_l.items():                
                    if "floorplan_" in key:
                        images_floor.append(value)
                if images_floor:
                    item_loader.add_value("floor_plan_images", images_floor)
            except:
                pass
       
        images = [
            response.urljoin(x)
            for x in response.xpath(
                "//a[@class='js--open woning__link']/img[contains(@class,'woning__image')]/@data-lazy-src"
            ).extract()
        ]
        if images:
            item_loader.add_value("images", images)
        
        
        item_loader.add_value("landlord_phone", "058 - 2337333")
        item_loader.add_value("landlord_name", "Makelaardijhoekstra")
        item_loader.add_value("landlord_email", "info@makelaardijhoekstra.nl")

        if item_loader.get_collected_values("property_type"):
            yield item_loader.load_item()
        
def split_address(address, get):
    temp = address.split(" ")[0]+" "+address.split(" ")[1]
    zip_code = temp
    city = address.split(temp)[1]

    if get == "zip":
        return zip_code
    else:
        return city

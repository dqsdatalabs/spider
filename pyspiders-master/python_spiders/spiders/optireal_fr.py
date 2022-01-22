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
    name = 'optireal_fr'
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"location-meublee":True, "location-vide":False}

        for key, value in kwargs.items():
            formdata = {
                "action": "wpestate_custom_adv_ajax_filter_listings_search",
                "val_holder[]": key,
                "val_holder[]": "",
                "val_holder[]": "all",
                "val_holder[]": "",
                "newpage": "1",
                "postid": "1",
                "halfmap": "1",
                "all_checkers": "",
                "filter_search_action10": "",
                "adv_location10": "",
                "filter_search_action11": "",
                "filter_search_categ11": "",
                "keyword_search": "",
                "geo_lat": "",
                "geo_long": "",
                "geo_rad": "",
                "order": "0",
            }
            yield FormRequest("https://www.france-ermitage.fr/wp-admin/admin-ajax.php",
                            callback=self.parse,
                            formdata=formdata,
                            dont_filter=True,
                            meta={'furnished': value})


    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        sel = Selector(text=data["cards"], type="html")
        for item in sel.xpath("//div[@class='item active']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'furnished': response.meta['furnished']})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        sale =  "".join(response.xpath("//div[@class='property_categs']/a/text()").extract())
        if "Vente" not in sale:
            furnished = response.meta.get('furnished')

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source","Optireal_PySpider_france")
            
            desc = "".join(response.xpath("//h4[.='Description']/following-sibling::p[1]/text()").getall())
            if get_p_type_string(desc):
                item_loader.add_value("property_type", get_p_type_string(desc))
            else:
                return

            title = response.xpath("//title/text()").get()
            if title:
                item_loader.add_value("title", title)

            rent = "".join(response.xpath("//span[@class='price_area']/text()").getall())
            if rent:
                price = rent.replace(" ","").split("€")[0].strip().replace(",",".").replace(".","")
                item_loader.add_value("rent", price)
            item_loader.add_value("currency", "EUR")

            room = response.xpath("//h1[@class='entry-title entry-prop']/text()").extract_first()
            if room and room.strip() !='0':
                if "chambre" in room:
                    item_loader.add_value("room_count", room.strip().split(" ")[0].strip())
                elif "Studio" in room:
                    item_loader.add_value("room_count", "1")

            square_meters = " ".join(response.xpath("substring-before(//h1[@class='entry-title entry-prop']/text()[contains(.,'m²')],'m²')").getall()).strip()   
            if square_meters:
                item_loader.add_value("square_meters", square_meters.split("–")[1].strip())

            address = " ".join(response.xpath("substring-after(//h1[@class='entry-title entry-prop']/text()[contains(.,'m²')],'m²')").getall()).strip()   
            if address:
                address = address.replace("–","").strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", address.split(" ")[-1].strip())
                # item_loader.add_value("zipcode", address.split(" ")[0].strip())

            utilities = "".join(response.xpath("//div[@class='wpestate_property_description']/p[contains(.,'Charges')]/text()[2]").getall())
            if utilities:
                item_loader.add_value("utilities", utilities.strip().split("€")[0].split(":")[1].strip())

            LatLng = "".join(response.xpath("substring-before(substring-after(//script/text()[contains(.,'latitude')],'general_latitude'),',')").getall())
            if LatLng:
                lat = LatLng.split('":"')[1].replace('"',"")
                lng = "".join(response.xpath("substring-before(substring-after(//script/text()[contains(.,'longitude')],'longitude'),',')").getall())
                lng2 = lng.split('":"')[1].replace('"',"")
                item_loader.add_value("latitude", lat)
                item_loader.add_value("longitude", lng2)


            item_loader.add_xpath("energy_label", "//div[contains(@class,'class-energy')]/div/div/@data-energyclass")

            description = " ".join(response.xpath("//div[@class='wpestate_property_description']/p/text()").getall()).strip()   
            if description:
                item_loader.add_value("description", description.strip())

            images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[@class='gallery_wrapper']/div/@style").getall()]
            if images:
                item_loader.add_value("images", images)

            balcony =" ".join(response.xpath("//ul[@class='amenities']/li[contains(.,'balcon')]//text()").getall()).strip() 
            if balcony:
                item_loader.add_value("balcony", True)

            elevator =" ".join(response.xpath("//ul[@class='amenities']/li[contains(.,'ascenseur')]//text()").getall()).strip() 
            if elevator:
                item_loader.add_value("elevator", True)

            washing_machine =" ".join(response.xpath("//ul[@class='amenities']/li[contains(.,'machine à laver')]//text()").getall()).strip() 
            if washing_machine:
                item_loader.add_value("washing_machine", True)

            parking =" ".join(response.xpath("//ul[@class='amenities']/li[contains(.,'parking')]//text()").getall()).strip() 
            if parking:
                item_loader.add_value("parking", True)

            if furnished == True:
                item_loader.add_value("furnished", True)
            elif furnished== False:
                item_loader.add_value("furnished", False)

            item_loader.add_value("landlord_phone", "01 42 56 23 42")
            item_loader.add_value("landlord_email", "contact@france-ermitage.fr")
            item_loader.add_xpath("landlord_name", "//div[@class='agent_contanct_form_sidebar']/div//h4/a/text()")
            yield item_loader.load_item()
        
def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None


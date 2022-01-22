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
    name = 'bespokerealtygroup_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'authority': 'bespokerealtygroup.com.au',
        'content-length': '0',
        'accept': '*/*',
        'origin': 'https://bespokerealtygroup.com.au',
        'referer': 'https://bespokerealtygroup.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&doing_wp_cron=1615435704.6008739471435546875000&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&minbaths=&maxbaths=&cars=&type=&subcategory=&externalID=&minbuildarea=&maxbuildarea=&buildareaunit=&minlandarea=&maxlandarea=&landareaunit=&order=&search=',
        'accept-language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        start_url = "https://bespokerealtygroup.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1615435704.6008739471435546875000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D=1&query%5Bextended%5D%5Bvalue%5D=1&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bminbaths%5D%5Bvalue%5D=&query%5Bmaxbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5Btype%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminbuildarea%5D%5Bvalue%5D=&query%5Bmaxbuildarea%5D%5Bvalue%5D=&query%5Bbuildareaunit%5D%5Bvalue%5D=&query%5Bminlandarea%5D%5Bvalue%5D=&query%5Bmaxlandarea%5D%5Bvalue%5D=&query%5Blandareaunit%5D%5Bvalue%5D=&query%5Border%5D%5Bvalue%5D=&query%5Bsearch%5D%5Bvalue%5D="
        yield FormRequest(start_url, headers=self.headers, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False

        data = json.loads(response.body)
        for item in data["data"]["listings"]:
            seen = True
            follow_url = item["url"]
            property_type = item["post_content"]
            if property_type:
                if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type":get_p_type_string(property_type), "item":item})

        if page == 2 or seen: 
            f_url = f"https://bespokerealtygroup.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bdoing_wp_cron%5D%5Bvalue%5D=1615435704.6008739471435546875000&query%5Bdoing_wp_cron%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={page}&query%5Bextended%5D%5Bvalue%5D=1&query%5Bminprice%5D%5Bvalue%5D=&query%5Bmaxprice%5D%5Bvalue%5D=&query%5Bminbeds%5D%5Bvalue%5D=&query%5Bmaxbeds%5D%5Bvalue%5D=&query%5Bminbaths%5D%5Bvalue%5D=&query%5Bmaxbaths%5D%5Bvalue%5D=&query%5Bcars%5D%5Bvalue%5D=&query%5Btype%5D%5Bvalue%5D=&query%5Bsubcategory%5D%5Bvalue%5D=&query%5BexternalID%5D%5Bvalue%5D=&query%5Bminbuildarea%5D%5Bvalue%5D=&query%5Bmaxbuildarea%5D%5Bvalue%5D=&query%5Bbuildareaunit%5D%5Bvalue%5D=&query%5Bminlandarea%5D%5Bvalue%5D=&query%5Bmaxlandarea%5D%5Bvalue%5D=&query%5Blandareaunit%5D%5Bvalue%5D=&query%5Border%5D%5Bvalue%5D=&query%5Bsearch%5D%5Bvalue%5D="
            yield FormRequest(f_url, headers=self.headers, callback=self.parse, meta={"page": page + 1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bespokerealtygroup_Com_PySpider_australia")
        
        title = response.xpath("//p[contains(@class,'single-listing-address')]/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("city", title.split(",")[-1].strip())
        
        rent = response.xpath("//p[@class='listing-info-price']/text()").get()
        if rent:
            price = rent.split(" ")[0].replace("$","")
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//p[contains(@class,'bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        room_count = response.xpath("//p[contains(@class,'bath')]/text()").get()
        if room_count:
            item_loader.add_value("bathroom_count", room_count)

        zipcode = "".join(response.xpath("substring-before(substring-after(//div[@class='screen-overlay']/script/text(),'postAddress = '),';')").extract())
        if zipcode:
            zipcode = zipcode.replace('"',"").strip().split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
        
        parking = response.xpath("//p[contains(@class,'car')]/text()[.!='0'] | //li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit = response.xpath("//strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("$","").strip())
        
        external_id = response.xpath("//strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng:')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat:')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('lng:')[1].split('}')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        import dateparser
        available_date = response.xpath("//strong[contains(.,'date available')]/following-sibling::text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        description = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x.split("'")[1] for x in response.xpath("//div[contains(@class,'slides')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Bespoke Realty Group")
        item_loader.add_value("landlord_phone", "02 4737 9977")
        item_loader.add_value("landlord_email", "hello@bespokerg.com.au")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None
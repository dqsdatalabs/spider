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
    name = 'edwardthomas_com_au'
    execution_type='testing'
    country='australia'
    locale='en'    
    
    post_url = "https://edwardthomas.com.au/wp-admin/admin-ajax.php?action=get_posts&query%5Bpost_type%5D%5Bvalue%5D=listings&query%5Bcount%5D%5Bvalue%5D=20&query%5Borderby%5D%5Bvalue%5D=meta_value&query%5Bmeta_key%5D%5Bvalue%5D=dateListed&query%5Bsold%5D%5Bvalue%5D=0&query%5BsaleOrRental%5D%5Bvalue%5D=Rental&query%5BsaleOrRental%5D%5Btype%5D=equal&query%5Bpaged%5D%5Bvalue%5D={}"
    def start_requests(self):
        formdata = {
            "action": "get_posts",
            "query[post_type][value]": "listings",
            "query[count][value]": "99999",
            "query[orderby][value]": "meta_value",
            "query[meta_key][value]": "dateListed",
            "query[sold][value]": "0",
            "query[saleOrRental][value]": "Rental",
            "query[saleOrRental][type]": "equal",
            "query[paged][value]": "1",
        }
        yield FormRequest(
            url=self.post_url.format(1),
            callback=self.parse,
            formdata=formdata,
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)["data"]
        for item in data["listings"]:
            status = item["status"]
            if status and "available" not in status.lower():
                continue
            follow_url = item["url"]
            yield Request(follow_url, callback=self.populate_item)
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        p_type = "".join(response.xpath("//h5[@class='single-post-title']/text()").getall())
        if get_p_type_string(p_type):
            p_type = get_p_type_string(p_type)
            item_loader.add_value("property_type", p_type)
        else:
            p_type = "".join(response.xpath("//div[contains(@class,'post-content')]/p//text()").getall())
            if get_p_type_string(p_type):
                p_type = get_p_type_string(p_type)
                item_loader.add_value("property_type", p_type)
            else:
                return
        item_loader.add_value("external_source", "Edwardthomas_Com_PySpider_australia") 
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//div[@class='listing-info-bar clearfix']//p[@class='listing-info-price']/text()").getall())
        if rent:
            if "per week" in rent:
                price = rent.split(" ")[0].strip().replace("$","").strip()
            else:
                price = rent.split("$")[1].strip().split(" ")[0].replace("p/w","").replace(",","").replace("pw","").strip()
            item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "USD")

        deposit = "".join(response.xpath("//div[@class='section-header']/div[@class='single-listing-keydetail']/strong[.='bond price']/following-sibling::text()").getall())
        if deposit:
            dep = deposit.split("$")[1].strip()
            item_loader.add_value("deposit", dep)

        address = "".join(response.xpath("//div[@class='b-address__text']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        item_loader.add_xpath("city", "substring-after(//div[@class='b-address__text']/text(),', ')")

        item_loader.add_xpath("city","substring-after(//p[@class='single-listing-address']/text(),', ')")
        item_loader.add_xpath("room_count","normalize-space(//div[@class='listing-attr-bar']/p[@class='listing-attr icon-bed']/text())")
        item_loader.add_xpath("bathroom_count","normalize-space(//div[@class='listing-attr-bar']/p[@class='listing-attr icon-bath']/text())")
        item_loader.add_xpath("external_id","normalize-space(//div[@class='single-listing-keydetail']/strong[.='property ID']/following-sibling::text())")

        description = " ".join(response.xpath("//div[@class='section-body post-content']/p/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        item_loader.add_xpath("latitude", "//div[@class='responsive-map']/@lat")
        item_loader.add_xpath("longitude", "//div[@class='responsive-map']/@lng")

        images = [x for x in response.xpath("//div[@id='media-gallery']/div/div/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date="".join(response.xpath("//div[@class='single-listing-keydetail']/strong[.='date available']/following-sibling::text()").getall())
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        parking = response.xpath("normalize-space(//div[@class='listing-attr-bar']/p[@class='listing-attr icon-car']/text())").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        item_loader.add_xpath("latitude", "substring-before(substring-after(//div[@class='section-body']/div/script,'lat: '),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(//div[@class='section-body']/div/script,'lng: '),'}')")

        balcony = response.xpath("//ul[@class='property-feature-list']/li/text()[.='Balcony']").get()
        if balcony:
            item_loader.add_value("balcony", True)

        dishwasher = response.xpath("//ul[@class='property-feature-list']/li/text()[.='Dishwasher']").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='agent-mb-content-wrapper']/h5/a/text())")
        item_loader.add_xpath("landlord_phone", "//p/strong[.='mobile']/following-sibling::text()")
        item_loader.add_xpath("landlord_email", "//p[@class='staff-mobile']/a/text()")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "town" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    else:
        return None
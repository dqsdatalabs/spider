# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
class MySpider(Spider):
    name = 'unihomes_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'   
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "accept-encoding": "gzip, deflate, br",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36"
        }
    start_urls = ['https://www.unihomes.co.uk/student-accommodation']  # LEVEL 1
    def start_requests(self):
        yield Request(
            url=self.start_urls[0],
            callback=self.jump,
        )

    def jump(self, response):
        for url in response.xpath("//h2[contains(.,'City')]/following-sibling::ul[@class='sitemap_ul'][1]/li/a/@href").getall():
            yield Request(url, callback=self.parse,headers=self.headers)

    def parse(self, response):
        for item in response.xpath("//div[@id='properties_list']//div[contains(@class,'property listing')]"):
            follow_url = item.xpath("./a/@href").get()
            prop_type = item.xpath(".//h2//b/text()").get()
            property_type = ""
            if get_p_type_string(prop_type):
                property_type = get_p_type_string(prop_type)
            if property_type:
                yield Request(follow_url, callback=self.populate_item,headers=self.headers, meta={"property_type": property_type})
           
        next_page = response.xpath("//li[@class='page-item']/a[@rel='next']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse,headers=self.headers)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Unihomes_Co_PySpider_united_kingdom")
       
        item_loader.add_xpath("title","//div[@class='col-12 py-10px']/p/text()")

        address = response.xpath("//div[@class='col-12 py-10px']/p/text()").get()
        if address:
            address = address.replace("\u00a0"," ")
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
    
        rent = response.xpath("//p[@class='font-size-16px']/strong/text()").get()
        if rent:
            rent = rent.replace("£","").replace(",","").strip()
            item_loader.add_value("rent", str(int(float(rent))*4))
        item_loader.add_value("currency", "GBP")
        deposit = response.xpath("//div[text()='Deposit:']/strong/text()[contains(.,'£')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(".")[0])
        room_count = response.xpath("//div[p[.='Bedrooms']]/p[i]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[p[.='Bathrooms']]/p[i]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        
        available_date = " ".join(response.xpath("//div[@id='availability']//div[@class='py-12px row']/div[1]//text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        
        description = " ".join(response.xpath("//div[@id='description']//p//text()[.!='Description']").getall())
        if description:
            item_loader.add_value("description", description.strip())
 
        parking = response.xpath("//div[@id='key_features']//p/text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        energy_label = response.xpath("//div[p[.='EPC Rating']]/p[2]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        lat_lng = response.xpath("//script[contains(.,'GeoCoordinates') and contains(.,'latitude')]/text()").get()
        if lat_lng:
            item_loader.add_value("latitude", lat_lng.split('"latitude": "')[-1].split('"')[0].strip())
            item_loader.add_value("longitude", lat_lng.split('"longitude": "')[-1].split('"')[0].strip())
        images = [response.urljoin(x) for x in response.xpath("//div[@class='swiper-wrapper']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "UniHomes")
        item_loader.add_value("landlord_phone", "03308220266")
        item_loader.add_value("landlord_email", "hello@unihomes.co.uk")
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None
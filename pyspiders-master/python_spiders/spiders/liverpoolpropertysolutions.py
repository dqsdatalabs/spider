# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy 
from ..loaders import ListingLoader
import re
from scrapy import Request,FormRequest
from word2number import w2n 
class LiverpoolpropertysolutionsSpider(scrapy.Spider):
    name = "liverpoolpropertysolutions"
    allowed_domains = ["liverpoolpropertysolutions.com"]
    start_urls = (
        'http://www.liverpoolpropertysolutions.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    custom_settings = { 
        "HTTPCACHE_ENABLED":False,
    }
    
    def start_requests(self):
        start_urls = [
            {'url': 'http://www.liverpoolpropertysolutions.com/?id=40821&action=view&route=search&view=list&input=L33&jengo_property_for=2&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_property_type=-1&jengo_radius=10&jengo_min_price=0&jengo_max_price=99999999999&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&longitude='
            }
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse,
                dont_filter=True
            )

    def parse(self, response, **kwargs):
       
        for item in response.xpath("//section[contains(@class,'bg-section-pad0')]"):
            follow_url = response.urljoin(item.xpath(".//a[.='View Details']/@href").extract_first())
            bedroom = item.xpath(".//li/a[i[@class='fa fa-bed']]/text()[normalize-space()]").extract_first()
            bathroom = item.xpath(".//li/a[i[@class='fa fa-bath']]/text()[normalize-space()]").extract_first()
            yield Request(follow_url, callback=self.get_property_details,meta={"bedroom" : bedroom, "bathroom" : bathroom })

        next_page = response.xpath("//div[@class='container text-center']/a[@class='next-prev']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse
            )    
    # def get_detail_urls(self, response):
    #     links = response.xpath('//article//h2/a')
    #     for link in links: 
    #         url = response.urljoin(link.xpath('./@href').extract_first())
    #         yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        property_type = " ".join(response.xpath("//span[@class='details-type']//text()").extract())
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        external_link = response.url
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', external_link.split("property/")[1].split("/")[0].strip())
        dontallow=response.url
        if dontallow and "commercial" in dontallow.lower():
            return 

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        rent_string = ''.join(response.xpath("//div[@class='container-slider']/h2/text()").extract())
        rent_string = re.sub(r'[\s]+', '', rent_string)
        item_loader.add_value('rent_string', rent_string)

        deposit = response.xpath("//div[@id='features']//text()[contains(.,'DEPOSIT')]").get()
        if deposit:
            deposit = deposit.split("DEPOSIT")[0].split(",")[-1].strip()
            if "MONTH" in deposit.upper():
                dep = deposit.strip().split(" ")[0].strip()      
                rent = rent_string.replace("£","").replace(",","").strip()
                number = w2n.word_to_num(dep)
                item_loader.add_value("deposit", int(rent)*number)

        address = ''.join(response.xpath("//h1[@class='details-address1']//text()").extract())
        if address:
            zipcode = address.split(', ')[-1]
            city = address.split(', ')[-2]
            item_loader.add_value('address', address)
            item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
        item_loader.add_value("room_count", str(response.meta.get('bedroom')))
        item_loader.add_value("bathroom_count", str(response.meta.get('bathroom')))
 

        images = [response.urljoin(x)for x in response.xpath("//div[@class='fotorama']//a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)
        features = response.xpath("//script[contains(.,'var features')]//text()").extract_first() 
        if features:
            features = features.split("features = '")[1].split("'")[0]
            if " furnished" in features.lower():
                item_loader.add_value('furnished', True)
            if "balcon" in features.lower():
                item_loader.add_value('balcony', True)
        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'parking') ]//text() | //div[@id='features']/text()[contains(.,'Parking')]").extract_first()
        if parking:
            item_loader.add_value('parking', True)
        script_map = response.xpath("//script[contains(.,'prop_lat = ')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("prop_lat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("prop_lng =")[1].split(";")[0].strip())
        description = ''.join(response.xpath("//section[@id='details']//div[@class='col-md-8']//p//text()").extract())
        if description:
            item_loader.add_value('description', description)

        item_loader.add_value('landlord_name', 'Liverpoolpropertysolutions')
        item_loader.add_value('landlord_email', 'info@liverpoolpropertysolutions.com')
        item_loader.add_value('landlord_phone', '0151 236 7771')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "room" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None
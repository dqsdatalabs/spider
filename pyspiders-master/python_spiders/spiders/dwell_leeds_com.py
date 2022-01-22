# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..helper import extract_number_only, format_date,remove_white_spaces
from ..loaders import ListingLoader
import js2xml
import lxml
from scrapy import Selector
import requests
from scrapy import Request,FormRequest
from datetime import datetime
import dateparser
class DwellLeedsComSpider(scrapy.Spider):
    name = "dwell-leeds_com"
    allowed_domains = ["dwell-leeds.com"]
    start_urls = (
        'http://dwell-leeds.com/let/property-to-let/',
    )
  
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position=0

    def start_requests(self):
        formdata = {
            "sortorder": "price-desc",
            "RPP": "12",
            "OrganisationId": "8e4494f9-05ff-4485-a4b0-7f8a5d69b351",
            "WebdadiSubTypeName": "Rentals",
            "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
            "includeSoldButton": "false",
        }
        yield FormRequest(
            url="https://dwell-leeds.com/api/set/results/list",
            callback=self.parse,
            dont_filter=True,
            formdata=formdata,
         
        )


    # 1. FOLLOWING
    def parse(self, response):
  
        for item in response.xpath("//div[@class='row property']//a[button[.='Full Details']]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.get_property_details)
                
        nextpage_num =response.xpath('//ul[@class="pagination"]//li[@class="current"]/following-sibling::li[1]/a/text()').get()
        if nextpage_num:
            formdata = {
                "sortorder": "price-desc",
                "RPP": "12",
                "OrganisationId": "8e4494f9-05ff-4485-a4b0-7f8a5d69b351",
                "WebdadiSubTypeName": "Rentals",
                "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
                "includeSoldButton": "false",
                "page": str(nextpage_num)
            }
            yield FormRequest(
                url="https://dwell-leeds.com/api/set/results/list",
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
            
            )         
    def get_property_details(self,response):
        item_loader = ListingLoader(response=response)

        property_type=response.url.split('/')[-2]
        if property_type=="flat":
            property_type="apartment"
        elif "house" in property_type or "Barn Conversion" in property_type:
            property_type="house"
        else: return
        item_loader.add_value('property_type',property_type)

        external_id=response.url.split("property/")[-1].split('/')[0]
        item_loader.add_value('external_id',external_id)

        title=response.xpath('//title/text()').extract_first()
        if title:
            title=title.split('|')[0].strip()
            item_loader.add_value('title',title)



        description = " ".join(response.xpath("//section[@id='description']//text()").getall()).strip()
        if description:
            description=remove_white_spaces(description)
            item_loader.add_value('description',description)

            description=description.replace('-'," ").replace('.',' ').replace(':',' ').replace("Deposits","Deposit").replace("="," ")
            description=remove_white_spaces(description)
            deposit_stringList=description.split("Deposit")
            if len(deposit_stringList)>1:
                deposit=deposit_stringList[-1].split()[0]
                if "£" in deposit:
                    item_loader.add_value('deposit',deposit)
                elif re.search(r'(?<=£)\s{0,1}\d+',deposit_stringList[-1]):
                    deposit_amount = re.search(r'(?<=£)\s{0,1}\d+',deposit_stringList[-1])
                    if deposit_amount:
                        item_loader.add_value('deposit',deposit_amount.group())

        
        available_date = "".join(response.xpath("//li[contains(.,'AVAILABLE')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.lower().split("available")[1].strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            

        address=response.xpath('//h1[@class="details_h1"]/text()').extract_first()
        if address: 
            address = address.strip()
            city=address.split(", ")[-3]
            item_loader.add_value('city',city)
            zipcode=address.split(", ")[-1]
            item_loader.add_value('zipcode',zipcode)           
            item_loader.add_value('address', re.sub("\s{2,}", " ", address))
        javascript = response.xpath('//script[contains(text(), "map")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude=xml_selector.xpath('//property[@name="lat"]//text()').extract_first()
            longitude=xml_selector.xpath('//property[@name="lng"]//text()').extract_first()
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)

        stats_string=response.xpath('//div[@class="details-stats"]//text()').extract()
        if stats_string:
            stats_string=remove_white_spaces(" ".join(stats_string)).split()
            item_loader.add_value('room_count',stats_string[0])
            item_loader.add_value('bathroom_count',stats_string[1])
        
        features=response.xpath('//ul[@class="featureslist"]//text()').extract()
        if features:
            featuresString=" ".join(features)

            # http://dwell-leeds.com/property/102203001977/ls8/leeds/oakwood-avenue/flat/1-bedroom
            if "parking" in featuresString.lower(): 
                item_loader.add_value('parking',True)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator',True)

            if "balcony" in featuresString.lower(): 
                item_loader.add_value('balcony',True)

            if "terrace" in featuresString.lower() or re.search(r'terrace',description.lower()): 
                item_loader.add_value('terrace',True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool',True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine',True)

            if "dishwasher" in featuresString.lower():
                item_loader.add_value('dishwasher',True)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed',True)

            # http://dwell-leeds.com/property/102203002743/ls9/leeds/quay-one/flat/1-bedroom
            if re.search(r'un[^\w]*furnish',featuresString.lower()) or re.search(r'un[^\w]*furnish',description.lower()):
                item_loader.add_value('furnished',False)
                
            elif re.search(r'not[^\w]*furnish',featuresString.lower()) or re.search(r'not[^\w]*furnish',description.lower()):
                item_loader.add_value('furnished',False)
                
            elif re.search(r'furnish',featuresString.lower()) or re.search(r'furnish',description.lower()):
                item_loader.add_value('furnished',True)


            # available_date_stringList=featuresString.split("Available from")
            # if len(available_date_stringList)>1:
            #     available_date_string=" ".join(available_date_stringList[-1].split()[0:3])
            #     if "now" not in available_date_string.lower():
            #         available_date_string=available_date_string.replace('st','').replace('rd','').replace('nd','').replace('th','')
            #         available_date=format_date(available_date_string,'%d %b %Y')
            #         if available_date_string != available_date:
            #             item_loader.add_value('available_date',available_date)


        rent_string=response.xpath('//span[contains(@class,"nativecurrency")]/text()').extract()
        if rent_string:
            rent_string="".join(rent_string)
            item_loader.add_value('rent_string',rent_string)

        images=response.xpath('//a[@class="fullScreenImage"]/@href').extract()
        if images:
            images=list(set(images))
            item_loader.add_value('images',images)

        floorPlanUrl=response.xpath('//a[contains(@class,"floorplan")]/@href').extract_first()
        if floorPlanUrl:
            floorPlanUrl=response.urljoin(floorPlanUrl)
            item_loader.add_value('floor_plan_images', Selector(text=requests.get(floorPlanUrl).text).xpath('//img[@id="fpimg"]/@src').extract_first())

        item_loader.add_value('landlord_name',"Dwell Leeds")
        item_loader.add_value('landlord_phone',' 0113 246 4860')
        item_loader.add_value('landlord_email','info@dwell-leeds.com')
        item_loader.add_value("external_source", "DwellLeeds_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link",response.url)

        city = response.xpath("//h1/span[@class='city']/text()").get()
        if city and not item_loader.get_collected_values("city"): item_loader.add_value("city", city.strip().replace(",", ""))

        zipcode = response.xpath("//h1/span[@class='displayPostCode']/text()").get()
        if zipcode and not item_loader.get_collected_values("zipcode"): item_loader.add_value("zipcode", zipcode.strip().replace(",", ""))

        address = " ".join(response.xpath("//h1//text()").getall()).strip()
        if address and not item_loader.get_collected_values("address"): 
            item_loader.add_value('address', re.sub("\s{2,}", " ", address))
    
        images = [response.urljoin(x.split("url(")[-1].split(")")[0].strip()) for x in response.xpath("//div[@id='imageViewerCarousel']/div/div/@style").getall()]
        if len(images) > 0 and not item_loader.get_collected_values("images"): item_loader.add_value("images", images)

        room_count = response.xpath("//img[contains(@src,'bedroom')]/../span/text()").get()
        if room_count and not item_loader.get_collected_values("room_count"): item_loader.add_value("room_count", room_count.strip())
    
        bathroom_count = response.xpath("//img[contains(@src,'bathroom')]/../span/text()").get()
        if bathroom_count and not item_loader.get_collected_values("bathroom_count"): item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters = response.xpath("//li[contains(.,'square feet')]/text()").get()
        if square_meters: 
            square_meters = "".join(filter(str.isnumeric, square_meters))
            square_meters = int(int(square_meters) * 0.09290304) if square_meters else square_meters
            if square_meters: item_loader.add_value("square_meters", square_meters)

        furnished = response.xpath("//li[contains(.,'FURNISHED')]//text()[not(contains(.,'UNFURNISHED'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        if response.xpath("//li[contains(.,'parking')]").get() and not item_loader.get_collected_values("parking"): item_loader.add_value("parking", True)

        latitude = response.xpath("//section[@id='maps']/@data-cords").get()
        if latitude and not item_loader.get_collected_values("latitude"): 
            item_loader.add_value("latitude", latitude.split('"lat":')[1].split(',')[0].replace('"', '').strip())
            item_loader.add_value("longitude", latitude.split('"lng":')[1].split('}')[0].replace('"', '').strip())

       
        self.position+=1
        item_loader.add_value("position",self.position)
        yield item_loader.load_item()

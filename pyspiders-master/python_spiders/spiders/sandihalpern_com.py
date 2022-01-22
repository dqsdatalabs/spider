# -*- coding: utf-8 -*-
# Author: Noor

import scrapy
from scrapy import FormRequest

from ..helper import sq_feet_to_meters
from ..loaders import ListingLoader

class MySpider(scrapy.Spider):
    name = 'sandihalpern_com'
    allowed_domains = ['sandihalpern.com']
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):
        form={'w4rapp': 'Y',
              'visitorId': '2d44651b'}
        yield FormRequest("https://sandihalpern.com/listings",
                             callback=self.parse,
                             headers={
                                'authority': 'sandihalpern.com'
                                ,'method':'POST'
                                ,'path': '/ajax_w4rapp_visitor_profile'
                                ,'scheme': 'https'
                                ,'accept':'*/*'
                                ,'accept - encoding': 'gzip,deflate,br'
                                ,'accept-language': 'en-US,en;q=0.9'
                                ,'content - length': 34
                                ,'content - type': 'application/x-www-form-urlencoded;charset=UTF-8'
                                ,'origin': 'https://sandihalpern.com'
                                ,'referer': 'https://sandihalpern.com/listings'
                                ,'sec-ch-ua': '"Chromium";v = "94","Google Chrome";v="94",";Not A Brand";v="99"'
                                ,'sec-ch-ua-mobile': '?0'
                                ,'sec-ch-ua-platform':'"Linux"'
                                ,'sec-fetch-dest': 'empty'
                                ,'sec-fetch-mode': 'cors'
                                ,'sec-fetch-site': 'same-origin'
                                ,'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36'
                                ,'x-requested-with': 'XMLHttpRequest'
                                ,'cookie': 'PHPSESSID=3f10f18ea4cbe3cd658157ed6207003e; __rf_traffic.SA-52B6-C18B=https%3A%2F%2Fc.findboliger.dk%2F; listinginstantaccesscookie=1; __atuvc=4%7C43; __atuvs=61767b16f0705ba7003; __rf_visitor.SA-52B6-C18B=2d44651b.1635152277010.1635155659222.2'
                             },
                            formdata=form
                          )

    def parse(self, response):
        for i in range (1,255):
            form={
                'advancesearch': 1,
                'currentwebpagename': 'listings',
                'listingstatus': 'forRent',
                'totalRecords': 5048,
                'page': i,
                'currentlimit': 30,
                'currentsorting': 'mylistingfirst',
                'clsectionid': 1
            }
            yield FormRequest(
                url='https://sandihalpern.com/listings',
                callback=self.parse2,
                dont_filter=True,
                formdata=form
            )

    def parse2(self, response):
        links = response.css('#loadmoremobile_1 a::attr(href)').extract()[:-2]
        links=list(dict.fromkeys(links))
        for link in links:
            yield scrapy.Request(
                url='https://sandihalpern.com/'+ link,
                callback=self.get_property_details,
                dont_filter=True
            )

    def get_property_details(self, response):
            item_loader = ListingLoader(response=response)
            item_loader.add_value('external_link', response.url)
            title = response.css('.listAdd::text').extract()[0][1:]
            item_loader.add_value('title', title)
           # item_loader.add_value('rent_string', rent[:rent.index(' ')])

            # images =response.css('img::attr(src)').extract()
            # item_loader.add_value('images', images)
            # item_loader.add_value('property_type', 'apartment')
            # address=response.css('#center h4::text').extract()[0]
            # item_loader.add_value('address',address)
            # item_loader.add_value('city',address.split(',')[-2])
            # item_loader.add_value('zipcode',address.split(',')[-1])
            # desc = response.css('p+ p::text').extract()[0]
            # item_loader.add_value('description', desc)
            # item_loader.add_value('landlord_name', 'Janice McDonald')
            # item_loader.add_value('landlord_phone', '604.729.4149')
            # item_loader.add_value('currency', 'CAD')
            # item_loader.add_value('external_source', self.external_source)
            #
            # info =response.css('#center li ::text').extract()
            # stripped_details = [i.strip().lower() for i in info]
            # if 'bedrooms' in stripped_details:
            #     i = stripped_details.index('bathrooms')
            #     v = stripped_details[i + 1]
            #     value=v[v.index(':')+2:]
            #     item_loader.add_value('room_count', int(value))
            # if 'bathrooms' in stripped_details:
            #     i = stripped_details.index('bathrooms')
            #     v = stripped_details[i + 1]
            #     value = v[v.index(':') + 2:]
            #     item_loader.add_value('bathroom_count', int(value))
            # if 'sqft' in stripped_details:
            #     i = stripped_details.index('sqft')
            #     v = stripped_details[i + 1]
            #     value = v[v.index(':') + 2:]
            #     item_loader.add_value('square_meters', int(int(sq_feet_to_meters(value))*10.764))
            # if 'available' in stripped_details:
            #     i = stripped_details.index('available')
            #     v = stripped_details[i + 1]
            #     value = v[v.index(':') + 2:]
            #     item_loader.add_value('available_date', value)
            yield item_loader.load_item()

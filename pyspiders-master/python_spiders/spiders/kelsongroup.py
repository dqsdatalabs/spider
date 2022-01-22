import scrapy
import re
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import dateutil.parser

class KelsongroupSpider(scrapy.Spider):
    name = 'kelsongroup'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['kelsongroup.com']
    start_urls = ['https://www.kelsongroup.com/properties']

    def parse(self, response):
        items,links = [],[]
        divs = divs = response.css(".property-item").getall()
        for div in divs:
            if re.findall("availability-pending",div):
                 items.append(div)
            elif re.findall("availability-true+",div):
                 items.append(div)
        
        
        for item in items:
            link = re.findall('/properties/.*/',item)
            if len(link) > 0:
                links.append(response.urljoin(link[0]))
        
        for link in links:
            yield scrapy.Request(url=link, callback=self.parse_page)
    
    def parse_page(self, response):
        i = 1
        for apartment in response.css(".suites .suite"):
            available_date      = apartment.css(".suite-availability .value::text").get()
            if available_date == None:
                available_date = apartment.css(".suite-availability .no-vacancy::text").get()
            

            if available_date == "Notify Me" or available_date == "No Vacancy":
                continue
            elif available_date == "Available Now":
                available_date = ""
            else:
                if available_date:
                    available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")
            
            room_count          = apartment.css(".suite-type .value::text").get()
            bathroom_count      = apartment.css(".suite-bath .value::text").get()
            rent                = apartment.css(".suite-rate .value::text").get().replace("$","")
            square_meters       = "".join(apartment.css(".suite-sqft .value::text").getall()).strip()
            property_type       = apartment.css(".suite-type .description-heading::text").get()
            description         = remove_white_spaces(" ".join(response.css(".content-item.details .cms-content p ::text").getall()))
            address             = response.css(".address .property-address ::text").get().strip().split(",")[0]
            city                = response.css(".address .property-address ::text").get().strip().split(",")[1].strip()
            zipcode             = response.css(".address .property-address ::text").get().strip().split(",")[-1].strip()
            landlord_name       = remove_white_spaces(response.css(".contact-name ::text").get())
            landlord_phone      = remove_white_spaces(response.css(".contact-number ::text").get())
            title               = response.css(".property-title ::text").get()
            images              = "|--|".join(response.css(".gallery-container section div a::attr(href)").getall())
            pets_allowed = appart_amenities(response.css(".widget.utilities ul li::attr(class)").getall())
            elevator,parking     = get_amenities(response.css(".property-amenities li::text").getall())
            balcony,washing_machine,dishwasher  = get_features(response.css(".suite_amenities ul li ::text").getall())
            if rent:
                rent = int(rent)
                
            if property_type:
                property_type = property_type.strip().split(" ")[0]
            
            if square_meters:
                square_meters = sq_feet_to_meters(make_int_val(square_meters))
            
            if room_count:
                if room_count.lower().strip() == "studio":
                    room_count = 1
                else:
                    room_count = int(re.findall("[0-9]+",room_count)[0].strip())
                
            if bathroom_count:
                bathroom_count = int(bathroom_count.replace("Bedroom","").strip())
            images = re.sub("https://cdn.theliftsystem.com/kelson/images/gallery/512/[0-9]+_kelsongroup_logo_fullcolour_cmyk-squarekg.jpg","",images).split("|--|")
            
            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("address"                ,address)
            item.add_value("external_link"          ,response.url+"#"+str(i))
            item.add_value("currency"               ,"CAD")
            item.add_value("images"                 ,images[:-1])
            item.add_value("title"                  ,title)
            item.add_value("rent"                   ,rent)
            item.add_value("elevator"               ,elevator)
            item.add_value("balcony"                ,balcony)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("dishwasher"             ,dishwasher)
            item.add_value("square_meters"          ,int(int(square_meters)*10.764))
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("property_type"          ,property_type)
            item.add_value("parking"                ,parking)
            item.add_value("pets_allowed"           ,pets_allowed)
            item.add_value("available_date"         ,available_date)
            item.add_value("description"            ,description)
            item.add_value("landlord_phone"         ,landlord_phone)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("zipcode"                ,zipcode)
            item.add_value("city"                   ,city)
            i+=1
            if room_count:
                yield item.load_item()


def make_int_val(square_meters):
    if '-' in square_meters:
        return round((int(square_meters.split("-")[0].strip().replace(",","").replace("$","")) + int(square_meters.split("-")[-1].strip().replace(",","").replace("$","")))/2)
    else:
        return int(square_meters.replace(",","").replace("$",""))

def get_amenities(val):
    elevator,parking = "",""
    for f in val:
        if "parking" in f.lower():
            parking =  True
        elif "elevator" in f.lower():
            elevator = True
    return elevator,parking

def appart_amenities(val):
    for f in val:
        if "pet" in f.lower():
            return True
    return ""

def get_features(val):
    balcony,Washer,Dishwasher = '','',''
    for f in val:
        if "balcon" in f.lower():
            balcony = True
        elif "dishwasher" in f.lower():
            Dishwasher = True
        elif "washer" in f.lower():
            Washer = True
    return balcony,Washer,Dishwasher
